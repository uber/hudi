/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie;

import com.google.common.collect.Iterables;
import com.uber.hoodie.common.HoodieClientTestUtils;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.ReadClientTestHelper;
import com.uber.hoodie.common.minicluster.HdfsTestService;
import com.uber.hoodie.common.model.HoodieCleaningPolicy;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodiePartitionMetadata;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.common.util.ParquetUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieRollbackException;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.io.compact.strategy.UnBoundedCompactionStrategy;
import com.uber.hoodie.table.HoodieTable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.uber.hoodie.common.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TestHoodieClient implements Serializable {

    private HoodieTableType tableType;

    public TestHoodieClient(HoodieTableType tType) {
        this.tableType = tType;
    }

    @Parameterized.Parameters(name = "HoodieTableTypes")
    public static Collection<HoodieTableType[]> data() {
        return Arrays.asList(new HoodieTableType[][]{
                {HoodieTableType.COPY_ON_WRITE},
                {HoodieTableType.MERGE_ON_READ}
        });
    }

    private transient JavaSparkContext jsc = null;
    private transient SQLContext sqlContext;
    private String basePath = null;
    private transient HoodieTestDataGenerator
            dataGen = null;

    //NOTE : Be careful in using DFS (FileSystem.class) vs LocalFs(RawLocalFileSystem.class)
    //The implementation and gurantees of many API's differ, for example check rename(src,dst)
    private static MiniDFSCluster dfsCluster;
    private static DistributedFileSystem dfs;
    private static HdfsTestService hdfsTestService;

    @Before
    public void init() throws IOException {

        // Initialize a local spark env
        //TODO(na) : fix filesystem close problem, workaround is to change to local[1] to run individual tests
        SparkConf sparkConf = new SparkConf().setAppName("TestHoodieClient").setMaster("local[1]");
        jsc = new JavaSparkContext(HoodieReadClient.addHoodieSupport(sparkConf));
        jsc.hadoopConfiguration().addResource(FSUtils.getFs().getConf());

        //SQLContext stuff
        sqlContext = new SQLContext(jsc);

        // Create a temp folder as the base path
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        basePath = folder.getRoot().getAbsolutePath();
        dfs.mkdirs(new Path(basePath));
        FSUtils.setFs(dfs);

        if(tableType == HoodieTableType.MERGE_ON_READ) {
            HoodieTestUtils.initTableType(basePath, HoodieTableType.MERGE_ON_READ);
        } else {
            HoodieTestUtils.init(basePath);
        }
        dataGen = new HoodieTestDataGenerator();
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        if (hdfsTestService != null) {
            hdfsTestService.stop();
            dfsCluster.shutdown();;
        }
        FSUtils.setFs(null);
        HoodieTestUtils.resetFS();
        FileSystem.closeAll();
    }

    @BeforeClass
    public static void setUpDFS() throws IOException {

        FileSystem.closeAll();
        if (hdfsTestService == null) {
            hdfsTestService = new HdfsTestService();
            dfsCluster = hdfsTestService.start(true);
            // Create a temp folder as the base path
            dfs = dfsCluster.getFileSystem();
        }
        FSUtils.setFs(dfs);
        HoodieTestUtils.resetFS();
    }


    private HoodieWriteConfig getConfig() {
        return getConfigBuilder().build();
    }

    private HoodieWriteConfig.Builder getConfigBuilder() {
        return HoodieWriteConfig.newBuilder().withPath(basePath)
                .withSchema(TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
                .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
                .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
                .forTable("test-trip-table").withIndexConfig(
                        HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build());
    }

    private void assertNoWriteErrors(List<WriteStatus> statuses) {
        // Verify there are no errors
        for (WriteStatus status : statuses) {
            assertFalse("Errors found in write of " + status.getFileId(), status.hasErrors());
        }
    }

    private void assertPartitionMetadata(String[] partitionPaths, FileSystem fs) throws IOException {
        for (String partitionPath: partitionPaths) {
            assertTrue(HoodiePartitionMetadata.hasPartitionMetadata(fs, new Path(basePath, partitionPath)));
            HoodiePartitionMetadata pmeta = new HoodiePartitionMetadata(fs, new Path(basePath, partitionPath));
            pmeta.readFromFS();
            assertEquals(3, pmeta.getPartitionDepth());
        }
    }

    private void checkTaggedRecords(List<HoodieRecord> taggedRecords, String commitTime) {
        for (HoodieRecord rec : taggedRecords) {
            assertTrue("Record " + rec + " found with no location.", rec.isCurrentLocationKnown());
            assertEquals("All records should have commit time "+ commitTime+", since updates were made",
                    rec.getCurrentLocation().getCommitTime(), commitTime);
        }
    }

    @Test
    public void testFilterExist() throws Exception {
        HoodieWriteConfig config = getConfig();
        HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);
        String newCommitTime = writeClient.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
        JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(records, 1);

        HoodieReadClient readClient = new HoodieReadClient(jsc, config.getBasePath());
        JavaRDD<HoodieRecord> filteredRDD = readClient.filterExists(recordsRDD);

        // Should not find any files
        assertTrue(filteredRDD.collect().size() == 100);

        JavaRDD<HoodieRecord> smallRecordsRDD = jsc.parallelize(records.subList(0, 75), 1);
        // We create three parquet file, each having one record. (two different partitions)
        List<WriteStatus> statuses = writeClient.bulkInsert(smallRecordsRDD, newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        readClient = new HoodieReadClient(jsc, config.getBasePath());
        filteredRDD = readClient.filterExists(recordsRDD);
        List<HoodieRecord> result = filteredRDD.collect();
        // Check results
        assertTrue(result.size() == 25);
    }

    @Test
    public void testAutoCommit() throws Exception {
        // Set autoCommit false
        HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
        Schema readerSchema = HoodieAvroUtils.addMetadataFields(Schema.parse(cfg.getSchema()));
        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);

        String newCommitTime = "001";
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

        JavaRDD<WriteStatus> result = client.bulkInsert(writeRecords, newCommitTime);

        assertFalse("If Autocommit is false, then commit should not be made automatically",
                HoodieTestUtils.doesCommitExist(basePath, newCommitTime));
        assertTrue("Commit should succeed", client.commit(newCommitTime, result));

        ReadClientTestHelper readClient = new ReadClientTestHelper(jsc, basePath, sqlContext, tableType, readerSchema);

        assertTrue("After explicit commit, commit file should be created",
                readClient.hasNewCommits("000"));

        newCommitTime = "002";
        records = dataGen.generateUpdates(newCommitTime, 100);
        JavaRDD<HoodieRecord> updateRecords = jsc.parallelize(records, 1);
        result = client.upsert(updateRecords, newCommitTime);
        assertFalse("If Autocommit is false, then commit should not be made automatically",
                HoodieTestUtils.doesCommitExist(basePath, newCommitTime));
        assertTrue("Commit should succeed", client.commit(newCommitTime, result));
        //re-init client to refresh commitTimeline
        readClient = new ReadClientTestHelper(jsc, basePath, sqlContext, tableType, readerSchema);
        assertTrue("After explicit commit, commit file should be created",
                readClient.hasNewCommits("001"));
    }

    @Test
    public void testUpserts() throws Exception {
        HoodieWriteConfig cfg = getConfig();
        Schema readerSchema = HoodieAvroUtils.addMetadataFields(Schema.parse(cfg.getSchema()));
        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);
        HoodieIndex index = HoodieIndex.createIndex(cfg, jsc);
        FileSystem fs = FSUtils.getFs();

        /**
         * Write 1 (only inserts)
         */
        String newCommitTime = "001";
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

        List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
        assertNoWriteErrors(statuses);

        // check the partition metadata is written out
        assertPartitionMetadata(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS, fs);

        // verify that there is a commit
        ReadClientTestHelper readClient = new ReadClientTestHelper(jsc, basePath, sqlContext, tableType, readerSchema);

        assertEquals("Expecting a single commit.", readClient.listCommitsSince("000").size(), 1);
        assertEquals("Latest commit should be 001",readClient.latestCommit(), newCommitTime);
        assertEquals("Must contain 200 records", readClient.readCommit(newCommitTime).size(), records.size());
        // Should have 100 records in table (check using Index), all in locations marked at commit
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig());

        List<HoodieRecord> taggedRecords = index.tagLocation(jsc.parallelize(records, 1), table).collect();
        checkTaggedRecords(taggedRecords, "001");

        /**
         * Write 2 (updates)
         */
        newCommitTime = "004";
        records = dataGen.generateUpdates(newCommitTime, 100);
        LinkedHashMap<HoodieKey, HoodieRecord> recordsMap = new LinkedHashMap<>();
        for (HoodieRecord rec : records) {
            if (!recordsMap.containsKey(rec.getKey())) {
                recordsMap.put(rec.getKey(), rec);
            }
        }
        List<HoodieRecord> dedupedRecords = new ArrayList<>(recordsMap.values());

        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        // verify there are now 2 commits
        // verify that there is a commit
        readClient = new ReadClientTestHelper(jsc, basePath, sqlContext, tableType, readerSchema);

        readClient.readCommit(newCommitTime);
        assertEquals("Expecting two commits.", readClient.listCommitsSince("000").size(), 2);
        assertEquals("Latest commit should be 004",readClient.latestCommit(), newCommitTime);

        metaClient = new HoodieTableMetaClient(fs, basePath);
        table = HoodieTable.getHoodieTable(metaClient, getConfig());

        // Index should be able to locate all updates in correct locations.
        taggedRecords = index.tagLocation(jsc.parallelize(dedupedRecords, 1), table).collect();
        // No index for MOR after upserts()
        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            checkTaggedRecords(taggedRecords, "004");
        }

        // Check the entire dataset has 100 records still
        String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
        for (int i=0; i < fullPartitionPaths.length; i++) {
            fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
        }
        assertEquals("Must contain 200 records", readClient.read(fullPartitionPaths).size(), 200);


        //NOTE : MOR can't read a specific commit at the moment (aka incremental pull)
        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            // Check that the incremental consumption from time 000
            assertEquals("Incremental consumption from time 002, should give all records in commit 004",
                    readClient.readCommit(newCommitTime).size(),
                    readClient.readSince("002").size());
            assertEquals("Incremental consumption from time 001, should give all records in commit 004",
                    readClient.readCommit(newCommitTime).size(),
                    readClient.readSince("001").size());
        }
    }

    @Test
    public void testDeletes() throws Exception {

        //Deletes for MOR don't work out of the box
        if(tableType == HoodieTableType.MERGE_ON_READ) {
            return;
        }
        HoodieWriteConfig cfg = getConfig();
        Schema readerSchema = HoodieAvroUtils.addMetadataFields(Schema.parse(cfg.getSchema()));
        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);
        HoodieIndex index = HoodieIndex.createIndex(cfg, jsc);
        FileSystem fs = FSUtils.getFs();

        /**
         * Write 1 (inserts and deletes)
         * Write actual 200 insert records and ignore 100 delete records
         */
        String newCommitTime = "001";
        List<HoodieRecord> fewRecordsForInsert = dataGen.generateInserts(newCommitTime, 200);
        List<HoodieRecord> fewRecordsForDelete = dataGen.generateDeletes(newCommitTime, 100);

        List<HoodieRecord> records = new ArrayList(fewRecordsForInsert);
        records.addAll(fewRecordsForDelete);

        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

        List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
        assertNoWriteErrors(statuses);

        ReadClientTestHelper readClient = new ReadClientTestHelper(jsc, basePath, sqlContext, tableType, readerSchema);

        assertEquals("Expecting a single commit.", readClient.listCommitsSince("000").size(), 1);
        assertEquals("Latest commit should be 001",readClient.latestCommit(), newCommitTime);
        assertEquals("Must contain 200 records", readClient.readCommit(newCommitTime).size(), fewRecordsForInsert.size());
        // Should have 100 records in table (check using Index), all in locations marked at commit
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig());

        List<HoodieRecord> taggedRecords = index.tagLocation(jsc.parallelize(fewRecordsForInsert, 1), table).collect();
        checkTaggedRecords(taggedRecords, "001");

        /**
         * Write 2 (deletes+writes)
         */
        newCommitTime = "004";
        fewRecordsForDelete = records.subList(0,50);
        List<HoodieRecord> fewRecordsForUpdate = records.subList(50,100);
        records = dataGen.generateDeletesFromExistingRecords(fewRecordsForDelete);

        records.addAll(fewRecordsForUpdate);

        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        // verify there are now 2 commits
        readClient = new ReadClientTestHelper(jsc, basePath, sqlContext, tableType, readerSchema);

        assertEquals("Expecting two commits.", readClient.listCommitsSince("000").size(), 2);
        assertEquals("Latest commit should be 004",readClient.latestCommit(), newCommitTime);

        metaClient = new HoodieTableMetaClient(fs, basePath);
        table = HoodieTable.getHoodieTable(metaClient, getConfig());

        // Check the entire dataset has 150 records(200-50) still
        String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
        for (int i=0; i < fullPartitionPaths.length; i++) {
            fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
        }

        assertEquals("Must contain 150 records", readClient.read(fullPartitionPaths).size(), 150);

        // Check that the incremental consumption from time 000
        assertEquals("Incremental consumption from latest commit, should give 50 updated records",
                readClient.readCommit(newCommitTime).size(),
                50);
        assertEquals("Incremental consumption from time 001, should give 50 updated records",
                50,
                readClient.readSince("001").size());
        assertEquals("Incremental consumption from time 000, should give 150",
                150,
                readClient.readSince("000").size());
    }


    @Test
    public void testCreateSavepoint() throws Exception {

        HoodieWriteConfig cfg = null;
        //Only works for MOR with compacted commits
        if(tableType == HoodieTableType.MERGE_ON_READ) {
            cfg = getConfigBuilder().withCompactionConfig(
                    HoodieCompactionConfig.newBuilder()
                            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(1)
                            .withInlineCompaction(true)
                            .withCompactionStrategy(new UnBoundedCompactionStrategy())
                            .withNumDeltaCommits(1) //This means every second deltacommit, a compaction commit will be created
                            .build()).build();

        } else {
            cfg = getConfigBuilder().withCompactionConfig(
                    HoodieCompactionConfig.newBuilder()
                            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(1)
                            .build()).build();

        }
        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);
        FileSystem fs = FSUtils.getFs();
        HoodieTestDataGenerator.writePartitionMetadata(fs, HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS, basePath);

        /**
         * Write 1 (only inserts)
         */
        String newCommitTime = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
        List<WriteStatus> statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        assertNoWriteErrors(statuses);

        /**
         * Write 2 (updates)
         */
        newCommitTime = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
        records = dataGen.generateUpdates(newCommitTime, records);
        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig());
        final String savepointedCommit = tableType == HoodieTableType.COPY_ON_WRITE ? newCommitTime : table.getCompactionCommitTimeline().lastInstant().get().getTimestamp();

        client.savepoint("hoodie-unit-test", "test");
        try {
            client.rollback(newCommitTime);
            fail("Rollback of a savepoint was allowed " + newCommitTime);
        } catch (HoodieRollbackException e) {
            // this is good
        }

        /**
         * Write 3 (updates)
         */
        newCommitTime = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
        records = dataGen.generateUpdates(newCommitTime, records);
        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        /**
         * Write 4 (updates)
         */
        newCommitTime = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
        records = dataGen.generateUpdates(newCommitTime, records);
        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        if(tableType == HoodieTableType.MERGE_ON_READ) {
            /**
             * Write 5 (updates) to create a new deltacommit (earlier commit turns into a compacted commit)
             */
            newCommitTime = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
            records = dataGen.generateUpdates(newCommitTime, records);
            statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
            // Verify there are no errors
            assertNoWriteErrors(statuses);
        }

        List<String> partitionPaths = FSUtils.getAllPartitionPaths(fs, cfg.getBasePath(), getConfig().shouldAssumeDatePartitioning());
        metaClient = new HoodieTableMetaClient(fs, basePath);
        table = HoodieTable.getHoodieTable(metaClient, getConfig());

        final TableFileSystemView view = tableType == HoodieTableType.COPY_ON_WRITE ? table.getFileSystemView() : table.getCompactedFileSystemView();

        List<HoodieDataFile> dataFiles = partitionPaths.stream().flatMap(s -> {
            Stream<List<HoodieDataFile>> files = view.getEveryVersionInPartition(s);
            return files.flatMap(Collection::stream).filter(f -> f.getCommitTime().equals(savepointedCommit));
        }).collect(Collectors.toList());

        assertEquals(StringUtils.format("The data files for commit %s should not be cleaned", savepointedCommit), 3, dataFiles.size());

        // Delete savepoint
        assertFalse(table.getCompletedSavepointTimeline().empty());
        client.deleteSavepoint(
                table.getCompletedSavepointTimeline().getInstants().findFirst().get().getTimestamp());

        metaClient = new HoodieTableMetaClient(fs, basePath);
        table = HoodieTable.getHoodieTable(metaClient, getConfig());
        // rollback and reupsert 004 will not work (because it renames .clean as well), upsert with a new commitTime
        String lastCommit = tableType == HoodieTableType.COPY_ON_WRITE ? newCommitTime : table.getActiveTimeline().lastInstant().get().getTimestamp();
        client.rollback(lastCommit);
        newCommitTime = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        final TableFileSystemView view1 = tableType == HoodieTableType.COPY_ON_WRITE ? table.getFileSystemView() : table.getCompactedFileSystemView();

        dataFiles = partitionPaths.stream().flatMap(s -> {
            Stream<List<HoodieDataFile>> files = view1.getEveryVersionInPartition(s);
            return files.flatMap(Collection::stream).filter(f -> f.getCommitTime().equals(savepointedCommit));
        }).collect(Collectors.toList());

        assertEquals(StringUtils.format("The data files for commit %s should be cleaned now", savepointedCommit), 0, dataFiles.size());
    }

    @Test
    public void testRollbackToSavepoint() throws Exception {

        HoodieWriteConfig cfg = null;
        //This can work for MOR with compacted commits
        if(tableType == HoodieTableType.MERGE_ON_READ) {
            cfg = getConfigBuilder().withCompactionConfig(
                    HoodieCompactionConfig.newBuilder()
                            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(1)
                            .withInlineCompaction(true)
                            .withCompactionStrategy(new UnBoundedCompactionStrategy())
                            .withNumDeltaCommits(1) //This means every second deltacommit, a compaction commit will be created
                            .build()).build();

        } else {
            cfg = getConfigBuilder().withCompactionConfig(
                    HoodieCompactionConfig.newBuilder()
                            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(1)
                            .build()).build();
        }

        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);
        FileSystem fs = FSUtils.getFs();
        HoodieTestDataGenerator.writePartitionMetadata(fs, HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS, basePath);

        /**
         * Write 1 (only inserts)
         */
        String newCommitTime1 = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime1, 200);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

        List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime1).collect();
        assertNoWriteErrors(statuses);

        /**
         * Write 2 (updates)
         */
        String newCommitTime2 = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
        records = dataGen.generateUpdates(newCommitTime2, records);
        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime2).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        client.savepoint("hoodie-unit-test", "test");

        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig());
        final String savepointedCommit = tableType == HoodieTableType.COPY_ON_WRITE ? newCommitTime2 : table.getCompactionCommitTimeline().lastInstant().get().getTimestamp();

        /**
         * Write 3 (updates)
         */
        String newCommitTime3 = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
        records = dataGen.generateUpdates(newCommitTime3, records);
        statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime3).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);
        List<String> partitionPaths = FSUtils.getAllPartitionPaths(fs, cfg.getBasePath(), getConfig().shouldAssumeDatePartitioning());
        metaClient = new HoodieTableMetaClient(fs, basePath);
        table = HoodieTable.getHoodieTable(metaClient, getConfig());
        final TableFileSystemView view1 = tableType == HoodieTableType.COPY_ON_WRITE ? table.getFileSystemView() : table.getCompactedFileSystemView();

        final String checkCommit1 = tableType == HoodieTableType.COPY_ON_WRITE ? newCommitTime3 : table.getCommitTimeline().nthFromLastInstant(1).get().getTimestamp();
        List<HoodieDataFile> dataFiles = partitionPaths.stream().flatMap(s -> {
            Stream<List<HoodieDataFile>> files = view1.getEveryVersionInPartition(s);
            return files.flatMap(Collection::stream).filter(f -> f.getCommitTime().equals(checkCommit1));
        }).collect(Collectors.toList());
        assertEquals(StringUtils.format("The data files for commit %s should be present", newCommitTime3), 3, dataFiles.size());


        String newCommitTime4 = null;
        if(tableType == HoodieTableType.COPY_ON_WRITE) { //TODO : fix rollback() for deltacommit and compaction commits
            /**
             * Write 4 (updates)
             */
            newCommitTime4 = HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date());
            records = dataGen.generateUpdates(newCommitTime4, records);
            statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime4).collect();
            // Verify there are no errors
            assertNoWriteErrors(statuses);

            metaClient = new HoodieTableMetaClient(fs, basePath);
            table = HoodieTable.getHoodieTable(metaClient, getConfig());
            final TableFileSystemView view2 = table.getFileSystemView();

            final String checkCommit2 = tableType == HoodieTableType.COPY_ON_WRITE ? newCommitTime4 : table.getCommitTimeline().lastInstant().get().getTimestamp();
            dataFiles = partitionPaths.stream().flatMap(s -> {
                Stream<List<HoodieDataFile>> files = view2.getEveryVersionInPartition(s);
                return files.flatMap(Collection::stream).filter(f -> f.getCommitTime().equals(checkCommit2));
            }).collect(Collectors.toList());
            assertEquals(StringUtils.format("The data files for commit %s should be present", newCommitTime4), 3, dataFiles.size());
        }


        // rolling back to a non existent savepoint must not succeed
        try {
            client.rollbackToSavepoint("001");
            fail("Rolling back to non-existent savepoint should not be allowed");
        } catch (HoodieRollbackException e) {
            // this is good
        }

        // rollback to savepoint 002 (newCommit2)
        HoodieInstant savepoint =
                table.getCompletedSavepointTimeline().getInstants().findFirst().get();
        client.rollbackToSavepoint(savepoint.getTimestamp());

        metaClient = new HoodieTableMetaClient(fs, basePath);
        table = HoodieTable.getHoodieTable(metaClient, getConfig());
        final TableFileSystemView view3 = tableType == HoodieTableType.COPY_ON_WRITE ? table.getFileSystemView() : table.getCompactedFileSystemView();
        dataFiles = partitionPaths.stream().flatMap(s -> {
            Stream<List<HoodieDataFile>> files = view3.getEveryVersionInPartition(s);
            return files.flatMap(Collection::stream).filter(f -> f.getCommitTime().equals(savepointedCommit));
        }).collect(Collectors.toList());
        assertEquals(StringUtils.format("The data files for commit %s be available", savepointedCommit), 3, dataFiles.size());

        dataFiles = partitionPaths.stream().flatMap(s -> {
            Stream<List<HoodieDataFile>> files = view3.getEveryVersionInPartition(s);
            return files.flatMap(Collection::stream).filter(f -> f.getCommitTime().equals(newCommitTime3));
        }).collect(Collectors.toList());
        assertEquals(StringUtils.format("The data files for commit %s should be rolled back", newCommitTime3), 0, dataFiles.size());

        if(tableType == HoodieTableType.COPY_ON_WRITE) { //For mergeonread we never upserted commit4
            final String commitTime4 = newCommitTime4;
            dataFiles = partitionPaths.stream().flatMap(s -> {
                Stream<List<HoodieDataFile>> files = view3.getEveryVersionInPartition(s);
                return files.flatMap(Collection::stream).filter(f -> f.getCommitTime().equals(commitTime4));
            }).collect(Collectors.toList());
            assertEquals(StringUtils.format("The data files for commit %s should be rolled back", newCommitTime4), 0, dataFiles.size());
        }
    }


    @Test
    public void testInsertAndCleanByVersions() throws Exception {

        int maxVersions = 2; // keep upto 2 versions for each file
        HoodieWriteConfig cfg = getConfigBuilder().withCompactionConfig(
                HoodieCompactionConfig.newBuilder()
                        .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS)
                        .retainFileVersions(maxVersions).build()).build();
        Schema readerSchema = HoodieAvroUtils.addMetadataFields(Schema.parse(TRIP_EXAMPLE_SCHEMA));
        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);
        HoodieIndex index = HoodieIndex.createIndex(cfg, jsc);
        FileSystem fs = FSUtils.getFs();

        /**
         * do a big insert
         * (this is basically same as insert part of upsert, just adding it here so we can
         * catch breakages in insert(), if the implementation diverges.)
         */
        String newCommitTime = client.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 500);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 5);

        List<WriteStatus> statuses = client.insert(writeRecords, newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        // verify that there is a commit

        ReadClientTestHelper readClient = new ReadClientTestHelper(jsc, basePath, sqlContext, tableType, readerSchema);

        assertEquals("Expecting a single commit.", readClient.listCommitsSince("000").size(), 1);
        // Should have 100 records in table (check using Index), all in locations marked at commit
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig());
        assertFalse(table.getCompletedCommitTimeline().empty());
        String commitTime =
                table.getCompletedCommitTimeline().getInstants().findFirst().get().getTimestamp();
        assertFalse(table.getCompletedCleanTimeline().empty());
        assertEquals("The clean instant should be the same as the commit instant", commitTime,
                table.getCompletedCleanTimeline().getInstants().findFirst().get().getTimestamp());

        List<HoodieRecord> taggedRecords = index.tagLocation(jsc.parallelize(records, 1), table).collect();
        checkTaggedRecords(taggedRecords, newCommitTime);

        // Keep doing some writes and clean inline. Make sure we have expected number of files remaining.
        for (int writeCnt = 2; writeCnt < 10; writeCnt++) {

            Thread.sleep(1100); // make sure commits are unique
            newCommitTime = client.startCommit();
            records = dataGen.generateUpdates(newCommitTime, 100);

            statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
            // Verify there are no errors
            assertNoWriteErrors(statuses);

            HoodieTableMetaClient metadata = new HoodieTableMetaClient(fs, basePath);
            table = HoodieTable.getHoodieTable(metadata, getConfig());
            HoodieTimeline timeline = table.getCommitTimeline();

            TableFileSystemView fsView = table.getFileSystemView();
            // Need to ensure the following
            for (String partitionPath : dataGen.getPartitionPaths()) {
                // compute all the versions of all files, from time 0
                HashMap<String, TreeSet<String>> fileIdToVersions = new HashMap<>();
                for (HoodieInstant entry : timeline.getInstants().collect(Collectors.toList())) {
                    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(entry).get());

                    for (HoodieWriteStat wstat : commitMetadata.getWriteStats(partitionPath)) {
                        if (!fileIdToVersions.containsKey(wstat.getFileId())) {
                            fileIdToVersions.put(wstat.getFileId(), new TreeSet<>());
                        }
                        fileIdToVersions.get(wstat.getFileId()).add(FSUtils.getCommitTime(new Path(wstat.getFullPath()).getName()));
                    }
                }

                List<List<HoodieDataFile>> fileVersions = fsView.getEveryVersionInPartition(partitionPath).collect(Collectors.toList());
                for (List<HoodieDataFile> entry : fileVersions) {
                    // No file has no more than max versions
                    String fileId = entry.iterator().next().getFileId();

                    assertTrue("fileId " + fileId + " has more than " + maxVersions + " versions",
                            entry.size() <= maxVersions);

                    // Each file, has the latest N versions (i.e cleaning gets rid of older versions)
                    List<String> commitedVersions = new ArrayList<>(fileIdToVersions.get(fileId));
                    for (int i = 0; i < entry.size(); i++) {
                        assertEquals("File " + fileId + " does not have latest versions on commits" + commitedVersions,
                                Iterables.get(entry, i).getCommitTime(),
                                commitedVersions.get(commitedVersions.size() - 1 - i));
                    }
                }
            }
        }
    }

    @Test
    public void testInsertAndCleanByCommits() throws Exception {

        int maxCommits = 3; // keep upto 3 commits from the past
        HoodieWriteConfig cfg = getConfigBuilder().withCompactionConfig(
                HoodieCompactionConfig.newBuilder()
                        .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS)
                        .retainCommits(maxCommits).build()).build();
        Schema readerSchema = HoodieAvroUtils.addMetadataFields(Schema.parse(TRIP_EXAMPLE_SCHEMA));
        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);
        HoodieIndex index = HoodieIndex.createIndex(cfg, jsc);
        FileSystem fs = FSUtils.getFs();

        /**
         * do a big insert
         * (this is basically same as insert part of upsert, just adding it here so we can
         * catch breakages in insert(), if the implementation diverges.)
         */
        String newCommitTime = client.startCommit();
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 500);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 5);

        List<WriteStatus> statuses = client.insert(writeRecords, newCommitTime).collect();
        // Verify there are no errors
        assertNoWriteErrors(statuses);

        // verify that there is a commit
        ReadClientTestHelper readClient = new ReadClientTestHelper(jsc, basePath, sqlContext, tableType, readerSchema);

        assertEquals("Expecting a single commit.", readClient.listCommitsSince("000").size(), 1);
        // Should have 100 records in table (check using Index), all in locations marked at commit
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig());

        assertFalse(table.getCompletedCommitTimeline().empty());
        String commitTime =
                table.getCompletedCommitTimeline().getInstants().findFirst().get().getTimestamp();
        assertFalse(table.getCompletedCleanTimeline().empty());
        assertEquals("The clean instant should be the same as the commit instant", commitTime,
                table.getCompletedCleanTimeline().getInstants().findFirst().get().getTimestamp());

        List<HoodieRecord> taggedRecords = index.tagLocation(jsc.parallelize(records, 1), table).collect();
        checkTaggedRecords(taggedRecords, newCommitTime);

        // Keep doing some writes and clean inline. Make sure we have expected number of files remaining.
        for (int writeCnt = 2; writeCnt < 10; writeCnt++) {
            Thread.sleep(1100); // make sure commits are unique
            newCommitTime = client.startCommit();
            records = dataGen.generateUpdates(newCommitTime, 100);

            statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
            // Verify there are no errors
            assertNoWriteErrors(statuses);

            HoodieTableMetaClient metadata = new HoodieTableMetaClient(fs, basePath);
            HoodieTable table1 = HoodieTable.getHoodieTable(metadata, cfg);
            HoodieTimeline activeTimeline = table1.getCompletedCommitTimeline();
            Optional<HoodieInstant>
                    earliestRetainedCommit = activeTimeline.nthFromLastInstant(maxCommits - 1);
            Set<HoodieInstant> acceptableCommits =
                    activeTimeline.getInstants().collect(Collectors.toSet());
            if (earliestRetainedCommit.isPresent()) {
                acceptableCommits.removeAll(
                        activeTimeline.findInstantsInRange("000", earliestRetainedCommit.get().getTimestamp()).getInstants()
                                .collect(Collectors.toSet()));
                acceptableCommits.add(earliestRetainedCommit.get());
            }

            TableFileSystemView fsView = table1.getCompactedFileSystemView();
            // Need to ensure the following
            for (String partitionPath : dataGen.getPartitionPaths()) {
                List<List<HoodieDataFile>> fileVersions = fsView.getEveryVersionInPartition(partitionPath).collect(Collectors.toList());
                for (List<HoodieDataFile> entry : fileVersions) {
                    Set<String> commitTimes = new HashSet<>();
                    for(HoodieDataFile value:entry) {
                        commitTimes.add(value.getCommitTime());
                    }
                    assertEquals("Only contain acceptable versions of file should be present",
                            acceptableCommits.stream().map(HoodieInstant::getTimestamp)
                                    .collect(Collectors.toSet()), commitTimes);
                }
            }
        }
    }

    @Test
    public void testRollbackCommit() throws Exception {

        // Let's create some commit files and parquet files
        String commitTime1 = "20160501010101";
        String commitTime2 = "20160502020601";
        String commitTime3 = "20160506030611";
        FSUtils.getFs().mkdirs(new Path(basePath + "/.hoodie"));
        HoodieTestDataGenerator.writePartitionMetadata(FSUtils.getFs(),
                new String[] {"2016/05/01", "2016/05/02", "2016/05/06"},
                basePath);

        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            // Only first two have commit files
            HoodieTestUtils.createCommitFiles(basePath, commitTime1, commitTime2);
            // Third one has a .inflight intermediate commit file
            HoodieTestUtils.createInflightCommitFiles(basePath, commitTime3);
        } else {
            HoodieTestUtils.createDeltaCommitFiles(basePath, commitTime1, commitTime2);
            // Third one has a .inflight intermediate commit file
            HoodieTestUtils.createDeltaInflightCommitFiles(basePath, commitTime3);
        }

        // Make commit1
        String file11 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime1, "id11");
        String file12 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime1, "id12");
        String file13 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime1, "id13");

        // Make commit2
        String file21 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime2, "id21");
        String file22 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime2, "id22");
        String file23 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime2, "id23");

        // Make commit3
        String file31 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime3, "id31");
        String file32 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime3, "id32");
        String file33 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime3, "id33");

        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
                .withIndexConfig(
                        HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY)
                                .build()).build();

        HoodieWriteClient client = new HoodieWriteClient(jsc, config, false);

        // Rollback commit 1 (this should fail, since commit2 is still around)
        try {
            client.rollback(commitTime1);
            assertTrue("Should have thrown an exception ", false);
        } catch (HoodieRollbackException hrbe) {
            // should get here
        }

        // Wait for 1 second to make sure we get a new timestamp for rollback commit
        Thread.sleep(1000);
        // Rollback commit3
        client.rollback(commitTime3);
        assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime3));
        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime3, file31) ||
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime3, file32) ||
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime3, file33));

        // simulate partial failure, where .inflight was not deleted, but data files were.
        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            HoodieTestUtils.createInflightCommitFiles(basePath, commitTime3);

        } else {
            HoodieTestUtils.createDeltaInflightCommitFiles(basePath, commitTime3);
        }

        Thread.sleep(1000);
        client.rollback(commitTime3);
        assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime3));

        Thread.sleep(1000);
        // Rollback commit2
        client.rollback(commitTime2);

        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            assertFalse(HoodieTestUtils.doesCommitExist(basePath, commitTime2));
        } else {
            assertFalse(HoodieTestUtils.doesDeltaCommitExist(basePath, commitTime2));
        }
        assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime2));
        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime2, file21) ||
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime2, file22) ||
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime2, file23));

        // simulate partial failure, where only .commit => .inflight renaming succeeded, leaving a
        // .inflight commit and a bunch of data files around.
        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            HoodieTestUtils.createInflightCommitFiles(basePath, commitTime2);

        } else {
            HoodieTestUtils.createDeltaInflightCommitFiles(basePath, commitTime2);
        }
        file21 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime2, "id21");
        file22 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime2, "id22");
        file23 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime2, "id23");

        Thread.sleep(1000);
        client.rollback(commitTime2);
        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            assertFalse(HoodieTestUtils.doesCommitExist(basePath, commitTime2));
        } else {
            assertFalse(HoodieTestUtils.doesDeltaCommitExist(basePath, commitTime2));
        }        assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime2));
        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime2, file21) ||
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime2, file22) ||
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime2, file23));


        Thread.sleep(1000);
        // Let's rollback commit1, Check results
        client.rollback(commitTime1);
        assertFalse(HoodieTestUtils.doesCommitExist(basePath, commitTime1));
        assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime1));
        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime1, file11) ||
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime1, file12) ||
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime1, file13));
    }


    @Test
    public void testAutoRollbackCommit() throws Exception {

        // Let's create some commit files and parquet files
        String commitTime1 = "20160501010101";
        String commitTime2 = "20160502020601";
        String commitTime3 = "20160506030611";
        FSUtils.getFs().mkdirs(new Path(basePath + "/.hoodie"));
        HoodieTestDataGenerator.writePartitionMetadata(FSUtils.getFs(),
                new String[] {"2016/05/01", "2016/05/02", "2016/05/06"},
                basePath);

        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            //Two good commits
            HoodieTestUtils.createCommitFiles(basePath, commitTime1, commitTime2);
            // One .inflight commit file
            HoodieTestUtils.createInflightCommitFiles(basePath, commitTime3);
        } else {
            //Two good commits
            HoodieTestUtils.createDeltaCommitFiles(basePath, commitTime1, commitTime2);
            // One .inflight commit file
            HoodieTestUtils.createDeltaInflightCommitFiles(basePath, commitTime3);
        }

        // Make commit1
        String file11 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime1, "id11");
        String file12 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime1, "id12");
        String file13 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime1, "id13");

        // Make commit2
        String file21 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime2, "id21");
        String file22 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime2, "id22");
        String file23 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime2, "id23");

        // Make commit3
        String file31 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime3, "id31");
        String file32 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime3, "id32");
        String file33 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime3, "id33");

        // Turn auto rollback off
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
                .withIndexConfig(
                        HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY)
                                .build()).build();

        new HoodieWriteClient(jsc, config, false);
        // Check results, nothing changed
        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            assertTrue(HoodieTestUtils.doesCommitExist(basePath, commitTime1));
            assertTrue(HoodieTestUtils.doesCommitExist(basePath, commitTime2));
            assertTrue(HoodieTestUtils.doesInflightExist(basePath, commitTime3));
        } else {
            assertTrue(HoodieTestUtils.doesDeltaCommitExist(basePath, commitTime1));
            assertTrue(HoodieTestUtils.doesDeltaCommitExist(basePath, commitTime2));
            assertTrue(HoodieTestUtils.doesDeltaInflightExist(basePath, commitTime3));
        }
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime3, file31) &&
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime3, file32) &&
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime3, file33));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime2, file21) &&
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime2, file22) &&
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime2, file23));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime1, file11) &&
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime1, file12) &&
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime1, file13));

        // Turn auto rollback on
        new HoodieWriteClient(jsc, config, true);
        if(tableType == HoodieTableType.COPY_ON_WRITE) {
            assertTrue(HoodieTestUtils.doesCommitExist(basePath, commitTime1));
            assertTrue(HoodieTestUtils.doesCommitExist(basePath, commitTime2));
        } else {
            assertTrue(HoodieTestUtils.doesDeltaCommitExist(basePath, commitTime1));
            assertTrue(HoodieTestUtils.doesDeltaCommitExist(basePath, commitTime2));
        }
        assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime3));
        assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime3, file31) ||
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime3, file32) ||
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime3, file33));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime2, file21) ||
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime2, file22) ||
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime2, file23));
        assertTrue(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime1, file11) &&
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime1, file12) &&
                HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime1, file13));
    }


    private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize) {
        HoodieWriteConfig.Builder builder = getConfigBuilder();
        return builder.withCompactionConfig(
                HoodieCompactionConfig.newBuilder()
                        .compactionSmallFileSize(HoodieTestDataGenerator.SIZE_PER_RECORD * 15)
                        .insertSplitSize(insertSplitSize).build()) // tolerate upto 15 records
                .withStorageConfig(HoodieStorageConfig.newBuilder()
                        .limitFileSize(HoodieTestDataGenerator.SIZE_PER_RECORD * 20)
                        .build())
                .build();
    }


    @Test
    public void testSmallInsertHandlingForUpserts() throws Exception {

        //MOR : Small upserts to existing parquet files don't work
        if(tableType == HoodieTableType.MERGE_ON_READ) {
            return;
        }

        FileSystem fs = FSUtils.getFs();
        final String TEST_PARTITION_PATH = "2016/09/26";
        final int INSERT_SPLIT_LIMIT = 10;
        // setup the small file handling params
        HoodieWriteConfig config = getSmallInsertWriteConfig(INSERT_SPLIT_LIMIT); // hold upto 20 records max
        dataGen = new HoodieTestDataGenerator(new String[] {TEST_PARTITION_PATH});

        HoodieWriteClient client = new HoodieWriteClient(jsc, config);

        // Inserts => will write file1
        String commitTime1 = "001";
        List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime1, INSERT_SPLIT_LIMIT); // this writes ~500kb
        Set<String> keys1 = HoodieClientTestUtils.getRecordKeys(inserts1);

        JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 1);
        List<WriteStatus> statuses= client.upsert(insertRecordsRDD1, commitTime1).collect();

        assertNoWriteErrors(statuses);

        assertEquals("Just 1 file needs to be added.", 1, statuses.size());
        String file1 = statuses.get(0).getFileId();
        assertEquals("file should contain 10 records",
                ParquetUtils.readRowKeysFromParquet(new Path(basePath, TEST_PARTITION_PATH + "/" + FSUtils.makeDataFileName(commitTime1, 0, file1))).size(),
                10);

        // Update + Inserts such that they just expand file1
        String commitTime2 = "002";
        List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime2, 4);
        Set<String> keys2 = HoodieClientTestUtils.getRecordKeys(inserts2);
        List<HoodieRecord> insertsAndUpdates2 = new ArrayList<>();
        insertsAndUpdates2.addAll(inserts2);
        insertsAndUpdates2.addAll(dataGen.generateUpdates(commitTime2, inserts1));

        JavaRDD<HoodieRecord> insertAndUpdatesRDD2 = jsc.parallelize(insertsAndUpdates2, 1);
        statuses = client.upsert(insertAndUpdatesRDD2, commitTime2).collect();
        assertNoWriteErrors(statuses);

        assertEquals("Just 1 file needs to be updated.", 1, statuses.size());
        assertEquals("Existing file should be expanded", file1, statuses.get(0).getFileId());
        assertEquals("Existing file should be expanded", commitTime1, statuses.get(0).getStat().getPrevCommit());
        Path newFile = new Path(basePath, TEST_PARTITION_PATH + "/" + FSUtils.makeDataFileName(commitTime2, 0, file1));
        assertEquals("file should contain 14 records", ParquetUtils.readRowKeysFromParquet(newFile).size(), 14);

        List<GenericRecord> records = ParquetUtils.readAvroRecords(newFile);
        for (GenericRecord record: records) {
            String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
            assertEquals("only expect commit2", commitTime2, record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());
            assertTrue("key expected to be part of commit2", keys2.contains(recordKey) || keys1.contains(recordKey));
        }

        // update + inserts such that file1 is updated and expanded, a new file2 is created.
        String commitTime3 = "003";
        List<HoodieRecord> insertsAndUpdates3 = dataGen.generateInserts(commitTime3, 20);
        Set<String> keys3 = HoodieClientTestUtils.getRecordKeys(insertsAndUpdates3);
        List<HoodieRecord> updates3 = dataGen.generateUpdates(commitTime3, inserts2);
        insertsAndUpdates3.addAll(updates3);

        JavaRDD<HoodieRecord> insertAndUpdatesRDD3 = jsc.parallelize(insertsAndUpdates3, 1);
        statuses = client.upsert(insertAndUpdatesRDD3, commitTime3).collect();
        assertNoWriteErrors(statuses);

        assertEquals("2 files needs to be committed.", 2, statuses.size());
        HoodieTableMetaClient metadata = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metadata, config);
        TableFileSystemView fileSystemView = table.getFileSystemView();
        List<HoodieDataFile> files = fileSystemView.getLatestVersionInPartition(TEST_PARTITION_PATH, commitTime3).collect(
                Collectors.toList());
        int numTotalInsertsInCommit3 = 0;
        for (HoodieDataFile file:  files) {
            if (file.getFileName().contains(file1)) {
                assertEquals("Existing file should be expanded", commitTime3, file.getCommitTime());
                records = ParquetUtils.readAvroRecords(new Path(file.getPath()));
                for (GenericRecord record: records) {
                    String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
                    String recordCommitTime = record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
                    if (recordCommitTime.equals(commitTime3)) {
                        if (keys2.contains(recordKey)) {
                            assertEquals("only expect commit3", commitTime3, recordCommitTime);
                            keys2.remove(recordKey);
                        } else {
                            numTotalInsertsInCommit3++;
                        }
                    }
                }
                assertEquals("All keys added in commit 2 must be updated in commit3 correctly", 0, keys2.size());
            } else {
                assertEquals("New file must be written for commit 3", commitTime3, file.getCommitTime());
                records = ParquetUtils.readAvroRecords(new Path(file.getPath()));
                for (GenericRecord record: records) {
                    String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
                    assertEquals("only expect commit3", commitTime3, record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());
                    assertTrue("key expected to be part of commit3", keys3.contains(recordKey));
                }
                numTotalInsertsInCommit3 += records.size();
            }
        }
        assertEquals("Total inserts in commit3 must add up", keys3.size(), numTotalInsertsInCommit3);
    }

    @Test
    public void testSmallInsertHandlingForInserts() throws Exception {

        //MOR : Small inserts to existing parquet files don't work
        if(tableType == HoodieTableType.MERGE_ON_READ) {
            return;
        }

        final String TEST_PARTITION_PATH = "2016/09/26";
        final int INSERT_SPLIT_LIMIT = 10;
        // setup the small file handling params
        HoodieWriteConfig config = getSmallInsertWriteConfig(INSERT_SPLIT_LIMIT); // hold upto 20 records max
        dataGen = new HoodieTestDataGenerator(new String[] {TEST_PARTITION_PATH});
        HoodieWriteClient client = new HoodieWriteClient(jsc, config);

        // Inserts => will write file1
        String commitTime1 = "001";
        List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime1, INSERT_SPLIT_LIMIT); // this writes ~500kb
        Set<String> keys1 = HoodieClientTestUtils.getRecordKeys(inserts1);
        JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 1);
        List<WriteStatus> statuses= client.insert(insertRecordsRDD1, commitTime1).collect();

        assertNoWriteErrors(statuses);
        assertPartitionMetadata(new String[]{TEST_PARTITION_PATH}, FSUtils.getFs());

        assertEquals("Just 1 file needs to be added.", 1, statuses.size());
        String file1 = statuses.get(0).getFileId();
        assertEquals("file should contain 10 records",
                ParquetUtils.readRowKeysFromParquet(new Path(basePath, TEST_PARTITION_PATH + "/" + FSUtils.makeDataFileName(commitTime1, 0, file1))).size(),
                10);

        // Second, set of Inserts should just expand file1
        String commitTime2 = "002";
        List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime2, 4);
        Set<String> keys2 = HoodieClientTestUtils.getRecordKeys(inserts2);
        JavaRDD<HoodieRecord> insertRecordsRDD2 = jsc.parallelize(inserts2, 1);
        statuses = client.insert(insertRecordsRDD2, commitTime2).collect();
        assertNoWriteErrors(statuses);

        assertEquals("Just 1 file needs to be updated.", 1, statuses.size());
        assertEquals("Existing file should be expanded", file1, statuses.get(0).getFileId());
        assertEquals("Existing file should be expanded", commitTime1, statuses.get(0).getStat().getPrevCommit());
        Path newFile = new Path(basePath, TEST_PARTITION_PATH + "/" + FSUtils.makeDataFileName(commitTime2, 0, file1));
        assertEquals("file should contain 14 records", ParquetUtils.readRowKeysFromParquet(newFile).size(), 14);

        List<GenericRecord> records = ParquetUtils.readAvroRecords(newFile);
        for (GenericRecord record: records) {
            String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
            String recCommitTime = record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
            assertTrue("Record expected to be part of commit 1 or commit2", commitTime1.equals(recCommitTime) || commitTime2.equals(recCommitTime));
            assertTrue("key expected to be part of commit 1 or commit2", keys2.contains(recordKey) || keys1.contains(recordKey));
        }

        // Lots of inserts such that file1 is updated and expanded, a new file2 is created.
        String commitTime3 = "003";
        List<HoodieRecord> insert3 = dataGen.generateInserts(commitTime3, 20);
        JavaRDD<HoodieRecord> insertRecordsRDD3 = jsc.parallelize(insert3, 1);
        statuses = client.insert(insertRecordsRDD3, commitTime3).collect();
        assertNoWriteErrors(statuses);
        assertEquals("2 files needs to be committed.", 2, statuses.size());


        FileSystem fs = FSUtils.getFs();
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable table = HoodieTable.getHoodieTable(metaClient, config);
        List<HoodieDataFile> files =
                table.getFileSystemView().getLatestVersionInPartition(TEST_PARTITION_PATH, commitTime3)
                        .collect(Collectors.toList());
        assertEquals("Total of 2 valid data files", 2, files.size());


        int totalInserts = 0;
        for (HoodieDataFile file:  files) {
            assertEquals("All files must be at commit 3", commitTime3, file.getCommitTime());
            records = ParquetUtils.readAvroRecords(new Path(file.getPath()));
            totalInserts += records.size();
        }
        assertEquals("Total number of records must add up", totalInserts, inserts1.size() + inserts2.size() + insert3.size());
    }

    @After
    public void clean() throws IOException {
        if (basePath != null) {
            new File(basePath).delete();
        }
        if (jsc != null) {
            jsc.stop();
        }
    }
}