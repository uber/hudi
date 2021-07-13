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

package org.apache.hudi.hadoop.realtime;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.RealtimeFileStatus;
import org.apache.hudi.hadoop.PathWithLogFilePath;
import org.apache.hudi.hadoop.HoodieEmptyRecordReader;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.UseFileSplitsFromInputFormat;
import org.apache.hudi.hadoop.UseRecordReaderFromInputFormat;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Input Format, that provides a real-time view of data in a Hoodie table.
 */
@UseRecordReaderFromInputFormat
@UseFileSplitsFromInputFormat
public class HoodieParquetRealtimeInputFormat extends HoodieParquetInputFormat implements Configurable {

  private static final Logger LOG = LogManager.getLogger(HoodieParquetRealtimeInputFormat.class);

  // To make Hive on Spark queries work with RT tables. Our theory is that due to
  // {@link org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher}
  // not handling empty list correctly, the ParquetRecordReaderWrapper ends up adding the same column ids multiple
  // times which ultimately breaks the query.

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

    Stream<FileSplit> fileSplits = Arrays.stream(super.getSplits(job, numSplits)).map(is -> (FileSplit) is);

    return HoodieRealtimeInputFormatUtils.getRealtimeSplits(job, fileSplits);
  }

  /**
   * keep the logical of mor_incr_view as same as spark datasource.
   * to do: unify the incremental view code between hive/spark-sql and spark datasource
   */
  @Override
  protected List<FileStatus> listStatusForIncrementalMode(
      JobConf job, HoodieTableMetaClient tableMetaClient, List<Path> inputPaths) throws IOException {
    List<FileStatus> result = new ArrayList<>();
    String tableName = tableMetaClient.getTableConfig().getTableName();
    Job jobContext = Job.getInstance(job);

    Option<HoodieTimeline> timeline = HoodieInputFormatUtils.getFilteredCommitsTimeline(jobContext, tableMetaClient);
    if (!timeline.isPresent()) {
      return result;
    }
    String lastIncrementalTs = HoodieHiveUtils.readStartCommitTime(jobContext, tableName);
    // Total number of commits to return in this batch. Set this to -1 to get all the commits.
    Integer maxCommits = HoodieHiveUtils.readMaxCommits(jobContext, tableName);
    HoodieTimeline commitsTimelineToReturn = timeline.get().findInstantsAfter(lastIncrementalTs, maxCommits);
    Option<List<HoodieInstant>> commitsToCheck = Option.of(commitsTimelineToReturn.getInstants().collect(Collectors.toList()));
    if (!commitsToCheck.isPresent()) {
      return result;
    }
    Map<String, HashMap<String, FileStatus>> partitionsWithFileStatus  = HoodieInputFormatUtils
        .listAffectedFilesForCommits(new Path(tableMetaClient.getBasePath()), commitsToCheck.get(), commitsTimelineToReturn);
    // build fileGroup from fsView
    List<FileStatus> affectedFileStatus = new ArrayList<>();
    partitionsWithFileStatus.forEach((key, value) -> value.forEach((k, v) -> affectedFileStatus.add(v)));
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(tableMetaClient, commitsTimelineToReturn, affectedFileStatus.toArray(new FileStatus[0]));
    // build fileGroup from fsView
    String basePath = tableMetaClient.getBasePath();
    // filter affectedPartition by inputPaths
    List<String> affectedPartition = partitionsWithFileStatus.keySet().stream()
        .filter(k -> k.isEmpty() ? inputPaths.contains(new Path(basePath)) : inputPaths.contains(new Path(basePath, k))).collect(Collectors.toList());
    if (affectedPartition.isEmpty()) {
      return result;
    }
    List<HoodieFileGroup> fileGroups = affectedPartition.stream()
        .flatMap(partitionPath -> fsView.getAllFileGroups(partitionPath)).collect(Collectors.toList());
    setInputPaths(job, affectedPartition.stream()
        .map(p -> p.isEmpty() ? basePath : new Path(basePath, p).toUri().toString()).collect(Collectors.joining(",")));

    // find all file status in current partitionPath
    FileStatus[] fileStatuses = getStatus(job);
    Map<String, FileStatus> candidateFileStatus = new HashMap<>();
    for (int i = 0; i < fileStatuses.length; i++) {
      String key = fileStatuses[i].getPath().toString();
      candidateFileStatus.put(key, fileStatuses[i]);
    }

    String maxCommitTime = fsView.getLastInstant().get().getTimestamp();
    fileGroups.stream().forEach(f -> {
      try {
        List<FileSlice> baseFiles = f.getAllFileSlices().filter(slice -> slice.getBaseFile().isPresent()).collect(Collectors.toList());
        if (!baseFiles.isEmpty()) {
          String baseFilePath = baseFiles.get(0).getBaseFile().get().getFileStatus().getPath().toUri().toString();
          if (!candidateFileStatus.containsKey(baseFilePath)) {
            throw new HoodieException("Error obtaining fileStatus for file: " + baseFilePath);
          }
          RealtimeFileStatus fileStatus = new RealtimeFileStatus(candidateFileStatus.get(baseFilePath));
          fileStatus.setMaxCommitTime(maxCommitTime);
          fileStatus.setBelongToIncrementalFileStatus(true);
          fileStatus.setBasePath(basePath);
          fileStatus.setBaseFilePath(baseFilePath);
          fileStatus.setDeltaLogPaths(f.getLatestFileSlice().get().getLogFiles().map(l -> l.getPath().toString()).collect(Collectors.toList()));
          result.add(fileStatus);
        }
        // add file group which has only logs.
        if (f.getLatestFileSlice().isPresent() && baseFiles.isEmpty()) {
          List<FileStatus> logFileStatus = f.getLatestFileSlice().get().getLogFiles().map(logFile -> logFile.getFileStatus()).collect(Collectors.toList());
          if (logFileStatus.size() > 0) {
            RealtimeFileStatus fileStatus = new RealtimeFileStatus(logFileStatus.get(0));
            fileStatus.setLogFileOnly(true);
            fileStatus.setBelongToIncrementalFileStatus(true);
            fileStatus.setDeltaLogPaths(logFileStatus.stream().map(l -> l.getPath().toString()).collect(Collectors.toList()));
            fileStatus.setMaxCommitTime(maxCommitTime);
            fileStatus.setBasePath(basePath);
            result.add(fileStatus);
          }
        }
      } catch (IOException e) {
        throw new HoodieException("Error obtaining data file/log file grouping ", e);
      }
    });
    return result;
  }

  @Override
  protected boolean includeLogFilesForSnapShortView() {
    return false;
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    if (filename instanceof PathWithLogFilePath) {
      return ((PathWithLogFilePath)filename).splitable();
    }
    return super.isSplitable(fs, filename);
  }

  @Override
  protected FileSplit makeSplit(Path file, long start, long length, String[] hosts) {
    if (file instanceof PathWithLogFilePath) {
      return ((PathWithLogFilePath)file).buildSplit(file, start, length, hosts);
    }
    return super.makeSplit(file, start, length, hosts);
  }

  @Override
  protected FileSplit makeSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts) {
    if (file instanceof PathWithLogFilePath) {
      return ((PathWithLogFilePath)file).buildSplit(file, start, length, hosts);
    }
    return super.makeSplit(file, start, length, hosts, inMemoryHosts);
  }

  @Override
  public FileStatus[] listStatus(JobConf job) throws IOException {
    // Call the HoodieInputFormat::listStatus to obtain all latest parquet files, based on commit
    // timeline.
    return super.listStatus(job);
  }

  @Override
  protected HoodieDefaultTimeline filterInstantsTimeline(HoodieDefaultTimeline timeline) {
    // no specific filtering for Realtime format
    return timeline;
  }

  void addProjectionToJobConf(final RealtimeSplit realtimeSplit, final JobConf jobConf) {
    // Hive on Spark invokes multiple getRecordReaders from different threads in the same spark task (and hence the
    // same JVM) unlike Hive on MR. Due to this, accesses to JobConf, which is shared across all threads, is at the
    // risk of experiencing race conditions. Hence, we synchronize on the JobConf object here. There is negligible
    // latency incurred here due to the synchronization since get record reader is called once per spilt before the
    // actual heavy lifting of reading the parquet files happen.
    if (HoodieRealtimeInputFormatUtils.canAddProjectionToJobConf(realtimeSplit, jobConf)) {
      synchronized (jobConf) {
        LOG.info(
            "Before adding Hoodie columns, Projections :" + jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR)
                + ", Ids :" + jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
        if (HoodieRealtimeInputFormatUtils.canAddProjectionToJobConf(realtimeSplit, jobConf)) {
          // Hive (across all versions) fails for queries like select count(`_hoodie_commit_time`) from table;
          // In this case, the projection fields gets removed. Looking at HiveInputFormat implementation, in some cases
          // hoodie additional projection columns are reset after calling setConf and only natural projections
          // (one found in select queries) are set. things would break because of this.
          // For e:g _hoodie_record_key would be missing and merge step would throw exceptions.
          // TO fix this, hoodie columns are appended late at the time record-reader gets built instead of construction
          // time.
          HoodieRealtimeInputFormatUtils.cleanProjectionColumnIds(jobConf);
          if (!realtimeSplit.getDeltaLogPaths().isEmpty()) {
            HoodieRealtimeInputFormatUtils.addRequiredProjectionFields(jobConf);
          }
          this.conf = jobConf;
          this.conf.set(HoodieInputFormatUtils.HOODIE_READ_COLUMNS_PROP, "true");
        }
      }
    }
  }

  @Override
  public RecordReader<NullWritable, ArrayWritable> getRecordReader(final InputSplit split, final JobConf jobConf,
                                                                   final Reporter reporter) throws IOException {
    // sanity check
    ValidationUtils.checkArgument(split instanceof RealtimeSplit,
        "HoodieRealtimeRecordReader can only work on RealtimeSplit and not with " + split);
    RealtimeSplit realtimeSplit = (RealtimeSplit) split;
    addProjectionToJobConf(realtimeSplit, jobConf);
    LOG.info("Creating record reader with readCols :" + jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR)
        + ", Ids :" + jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));

    // for log only split, we no need parquet reader, set it to empty
    if (realtimeSplit.getLogOnly()) {
      return new HoodieRealtimeRecordReader(realtimeSplit, jobConf, new HoodieEmptyRecordReader());
    }
    return new HoodieRealtimeRecordReader(realtimeSplit, jobConf,
        super.getRecordReader(split, jobConf, reporter));
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
