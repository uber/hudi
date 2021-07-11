/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.internal;

import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.testutils.SparkDatasetTestUtils.ENCODER;
import static org.apache.hudi.testutils.SparkDatasetTestUtils.STRUCT_TYPE;
import static org.apache.hudi.testutils.SparkDatasetTestUtils.getRandomRows;
import static org.apache.hudi.testutils.SparkDatasetTestUtils.toInternalRows;

/**
 * Unit tests {@link HoodieDataSourceInternalWriter}.
 */
public class TestHoodieDataSourceInternalWriter extends
    HoodieBulkInsertInternalWriterTestBase {

  private static Stream<Arguments> bulkInsertTypeParams() {
    Object[][] data = new Object[][] {
        {true},
        {false}
    };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("bulkInsertTypeParams")
  public void testDataSourceWriter(boolean populateMetaColumns) throws Exception {
    // init config and table
    HoodieWriteConfig cfg = getWriteConfig(populateMetaColumns);
    String instantTime = "001";
    // init writer
    HoodieDataSourceInternalWriter dataSourceInternalWriter =
        new HoodieDataSourceInternalWriter(instantTime, cfg, STRUCT_TYPE, sqlContext.sparkSession(), hadoopConf, populateMetaColumns, false);
    DataWriter<InternalRow> writer = dataSourceInternalWriter.createWriterFactory().createDataWriter(0, RANDOM.nextLong(), RANDOM.nextLong());

    String[] partitionPaths = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS;
    List<String> partitionPathsAbs = new ArrayList<>();
    for (String partitionPath : partitionPaths) {
      partitionPathsAbs.add(basePath + "/" + partitionPath + "/*");
    }

    int size = 10 + RANDOM.nextInt(1000);
    int batches = 5;
    Dataset<Row> totalInputRows = null;

    for (int j = 0; j < batches; j++) {
      String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[j % 3];
      Dataset<Row> inputRows = getRandomRows(sqlContext, size, partitionPath, false);
      writeRows(inputRows, writer);
      if (totalInputRows == null) {
        totalInputRows = inputRows;
      } else {
        totalInputRows = totalInputRows.union(inputRows);
      }
    }

    HoodieWriterCommitMessage commitMetadata = (HoodieWriterCommitMessage) writer.commit();
    List<HoodieWriterCommitMessage> commitMessages = new ArrayList<>();
    commitMessages.add(commitMetadata);
    dataSourceInternalWriter.commit(commitMessages.toArray(new HoodieWriterCommitMessage[0]));
    metaClient.reloadActiveTimeline();
    Dataset<Row> result = HoodieClientTestUtils.read(jsc, basePath, sqlContext, metaClient.getFs(), partitionPathsAbs.toArray(new String[0]));
    // verify output
    assertOutput(totalInputRows, result, instantTime, Option.empty(), populateMetaColumns);
    assertWriteStatuses(commitMessages.get(0).getWriteStatuses(), batches, size, Option.empty(), Option.empty());
  }

  @ParameterizedTest
  @MethodSource("bulkInsertTypeParams")
  public void testMultipleDataSourceWrites(boolean populateMetaColumns) throws Exception {
    // init config and table
    HoodieWriteConfig cfg = getWriteConfig(populateMetaColumns);
    int partitionCounter = 0;

    // execute N rounds
    for (int i = 0; i < 5; i++) {
      String instantTime = "00" + i;
      // init writer
      HoodieDataSourceInternalWriter dataSourceInternalWriter =
          new HoodieDataSourceInternalWriter(instantTime, cfg, STRUCT_TYPE, sqlContext.sparkSession(), hadoopConf, populateMetaColumns, false);

      List<HoodieWriterCommitMessage> commitMessages = new ArrayList<>();
      Dataset<Row> totalInputRows = null;
      DataWriter<InternalRow> writer = dataSourceInternalWriter.createWriterFactory().createDataWriter(partitionCounter++, RANDOM.nextLong(), RANDOM.nextLong());

      int size = 10 + RANDOM.nextInt(1000);
      int batches = 5; // one batch per partition

      for (int j = 0; j < batches; j++) {
        String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[j % 3];
        Dataset<Row> inputRows = getRandomRows(sqlContext, size, partitionPath, false);
        writeRows(inputRows, writer);
        if (totalInputRows == null) {
          totalInputRows = inputRows;
        } else {
          totalInputRows = totalInputRows.union(inputRows);
        }
      }

      HoodieWriterCommitMessage commitMetadata = (HoodieWriterCommitMessage) writer.commit();
      commitMessages.add(commitMetadata);
      dataSourceInternalWriter.commit(commitMessages.toArray(new HoodieWriterCommitMessage[0]));
      metaClient.reloadActiveTimeline();

      Dataset<Row> result = HoodieClientTestUtils.readCommit(basePath, sqlContext, metaClient.getCommitTimeline(), instantTime,
          populateMetaColumns);

      // verify output
      assertOutput(totalInputRows, result, instantTime, Option.empty(), populateMetaColumns);
      assertWriteStatuses(commitMessages.get(0).getWriteStatuses(), batches, size, Option.empty(), Option.empty());
    }
  }

  @ParameterizedTest
  @MethodSource("bulkInsertTypeParams")
  public void testLargeWrites(boolean populateMetaColumns) throws Exception {
    // init config and table
    HoodieWriteConfig cfg = getWriteConfig(populateMetaColumns);
    int partitionCounter = 0;

    // execute N rounds
    for (int i = 0; i < 3; i++) {
      String instantTime = "00" + i;
      // init writer
      HoodieDataSourceInternalWriter dataSourceInternalWriter =
          new HoodieDataSourceInternalWriter(instantTime, cfg, STRUCT_TYPE, sqlContext.sparkSession(), hadoopConf, populateMetaColumns, false);

      List<HoodieWriterCommitMessage> commitMessages = new ArrayList<>();
      Dataset<Row> totalInputRows = null;
      DataWriter<InternalRow> writer = dataSourceInternalWriter.createWriterFactory().createDataWriter(partitionCounter++, RANDOM.nextLong(), RANDOM.nextLong());

      int size = 10000 + RANDOM.nextInt(10000);
      int batches = 3; // one batch per partition

      for (int j = 0; j < batches; j++) {
        String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[j % 3];
        Dataset<Row> inputRows = getRandomRows(sqlContext, size, partitionPath, false);
        writeRows(inputRows, writer);
        if (totalInputRows == null) {
          totalInputRows = inputRows;
        } else {
          totalInputRows = totalInputRows.union(inputRows);
        }
      }

      HoodieWriterCommitMessage commitMetadata = (HoodieWriterCommitMessage) writer.commit();
      commitMessages.add(commitMetadata);
      dataSourceInternalWriter.commit(commitMessages.toArray(new HoodieWriterCommitMessage[0]));
      metaClient.reloadActiveTimeline();

      Dataset<Row> result = HoodieClientTestUtils.readCommit(basePath, sqlContext, metaClient.getCommitTimeline(), instantTime,
          populateMetaColumns);

      // verify output
      assertOutput(totalInputRows, result, instantTime, Option.empty(), populateMetaColumns);
      assertWriteStatuses(commitMessages.get(0).getWriteStatuses(), batches, size, Option.empty(), Option.empty());
    }
  }

  /**
   * Tests that DataSourceWriter.abort() will abort the written records of interest write and commit batch1 write and abort batch2 Read of entire dataset should show only records from batch1.
   * commit batch1
   * abort batch2
   * verify only records from batch1 is available to read
   */
  @ParameterizedTest
  @MethodSource("bulkInsertTypeParams")
  public void testAbort(boolean populateMetaColumns) throws Exception {
    // init config and table
    HoodieWriteConfig cfg = getWriteConfig(populateMetaColumns);

    String instantTime0 = "00" + 0;
    // init writer
    HoodieDataSourceInternalWriter dataSourceInternalWriter =
        new HoodieDataSourceInternalWriter(instantTime0, cfg, STRUCT_TYPE, sqlContext.sparkSession(), hadoopConf, populateMetaColumns, false);
    DataWriter<InternalRow> writer = dataSourceInternalWriter.createWriterFactory().createDataWriter(0, RANDOM.nextLong(), RANDOM.nextLong());

    List<String> partitionPaths = Arrays.asList(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS);
    List<String> partitionPathsAbs = new ArrayList<>();
    for (String partitionPath : partitionPaths) {
      partitionPathsAbs.add(basePath + "/" + partitionPath + "/*");
    }

    int size = 10 + RANDOM.nextInt(100);
    int batches = 1;
    Dataset<Row> totalInputRows = null;

    for (int j = 0; j < batches; j++) {
      String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[j % 3];
      Dataset<Row> inputRows = getRandomRows(sqlContext, size, partitionPath, false);
      writeRows(inputRows, writer);
      if (totalInputRows == null) {
        totalInputRows = inputRows;
      } else {
        totalInputRows = totalInputRows.union(inputRows);
      }
    }

    HoodieWriterCommitMessage commitMetadata = (HoodieWriterCommitMessage) writer.commit();
    List<HoodieWriterCommitMessage> commitMessages = new ArrayList<>();
    commitMessages.add(commitMetadata);
    // commit 1st batch
    dataSourceInternalWriter.commit(commitMessages.toArray(new HoodieWriterCommitMessage[0]));
    metaClient.reloadActiveTimeline();
    Dataset<Row> result = HoodieClientTestUtils.read(jsc, basePath, sqlContext, metaClient.getFs(), partitionPathsAbs.toArray(new String[0]));
    // verify rows
    assertOutput(totalInputRows, result, instantTime0, Option.empty(), populateMetaColumns);
    assertWriteStatuses(commitMessages.get(0).getWriteStatuses(), batches, size, Option.empty(), Option.empty());

    // 2nd batch. abort in the end
    String instantTime1 = "00" + 1;
    dataSourceInternalWriter =
        new HoodieDataSourceInternalWriter(instantTime1, cfg, STRUCT_TYPE, sqlContext.sparkSession(), hadoopConf, populateMetaColumns, false);
    writer = dataSourceInternalWriter.createWriterFactory().createDataWriter(1, RANDOM.nextLong(), RANDOM.nextLong());

    for (int j = 0; j < batches; j++) {
      String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[j % 3];
      Dataset<Row> inputRows = getRandomRows(sqlContext, size, partitionPath, false);
      writeRows(inputRows, writer);
    }

    commitMetadata = (HoodieWriterCommitMessage) writer.commit();
    commitMessages = new ArrayList<>();
    commitMessages.add(commitMetadata);
    // commit 1st batch
    dataSourceInternalWriter.abort(commitMessages.toArray(new HoodieWriterCommitMessage[0]));
    metaClient.reloadActiveTimeline();
    result = HoodieClientTestUtils.read(jsc, basePath, sqlContext, metaClient.getFs(), partitionPathsAbs.toArray(new String[0]));
    // verify rows
    // only rows from first batch should be present
    assertOutput(totalInputRows, result, instantTime0, Option.empty(), populateMetaColumns);
  }

  private void writeRows(Dataset<Row> inputRows, DataWriter<InternalRow> writer) throws Exception {
    List<InternalRow> internalRows = toInternalRows(inputRows, ENCODER);
    // issue writes
    for (InternalRow internalRow : internalRows) {
      writer.write(internalRow);
    }
  }
}
