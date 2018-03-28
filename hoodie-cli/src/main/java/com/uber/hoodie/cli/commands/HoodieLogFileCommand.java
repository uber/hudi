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

package com.uber.hoodie.cli.commands;

import com.beust.jcommander.internal.Maps;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.hoodie.cli.HoodieCLI;
import com.uber.hoodie.cli.HoodiePrintHelper;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.log.HoodieCompactedLogRecordScanner;
import com.uber.hoodie.common.table.log.HoodieLogFormat;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.log.block.HoodieCorruptBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieMemoryConfig;
import com.uber.hoodie.hive.util.SchemaUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;
import parquet.avro.AvroSchemaConverter;
import parquet.schema.MessageType;
import scala.Tuple2;
import scala.Tuple3;

@Component
public class HoodieLogFileCommand implements CommandMarker {

  @CliAvailabilityIndicator({"show logfiles"})
  public boolean isShowArchivedLogFileAvailable() {
    return HoodieCLI.tableMetadata != null;
  }

  @CliCommand(value = "show logfile metadata", help = "Read commit metadata from log files")
  public String showLogFileCommits(
      @CliOption(key = "logFilePathPattern", mandatory = true, help = "Fully qualified path for the log file")
      final String logFilePathPattern) throws IOException {

    FileSystem fs = HoodieCLI.tableMetadata.getFs();
    List<String> logFilePaths = Arrays.stream(fs.globStatus(new Path(logFilePathPattern)))
        .map(status -> status.getPath().toString()).collect(Collectors.toList());
    Map<String, List<Tuple3<HoodieLogBlockType, Tuple2<Map<HeaderMetadataType, String>, Map<HeaderMetadataType, String>>, Integer>>> commitCountAndMetadata = Maps
        .newHashMap();
    int totalEntries = 0;
    int numCorruptBlocks = 0;

    for (String logFilePath : logFilePaths) {
      FileStatus[] fsStatus = fs.listStatus(
          new Path(logFilePath));
      Schema writerSchema = null;
      MessageType messageType = SchemaUtil
          .readSchemaFromLogFile(HoodieCLI.tableMetadata.getFs(), new Path(logFilePath));
      // check if this message type is null, can happen when either the log file is empty or has
      // only corrupt blocks
      if(messageType != null) {
        writerSchema = new AvroSchemaConverter()
            .convert(messageType);
      }
      HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(fs,
          new HoodieLogFile(fsStatus[0].getPath()), writerSchema);

      // read the avro blocks
      while (reader.hasNext()) {
        HoodieLogBlock n = reader.next();
        String instantTime;
        int recordCount = 0;
        if (n instanceof HoodieCorruptBlock) {
          try {
            instantTime = n.getLogBlockHeader().get(HeaderMetadataType.INSTANT_TIME);
            writerSchema = Schema.parse(n.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));
          } catch (Exception e) {
            numCorruptBlocks++;
            instantTime = "corrupt_block_" + numCorruptBlocks;
            // could not read metadata for corrupt block
          }
        } else {
          instantTime = n.getLogBlockHeader().get(HeaderMetadataType.INSTANT_TIME);
          if (n instanceof HoodieAvroDataBlock) {
            recordCount = ((HoodieAvroDataBlock) n).getRecords().size();
          }
        }
        if (commitCountAndMetadata.containsKey(instantTime)) {
          commitCountAndMetadata.get(instantTime)
              .add(new Tuple3<>(n.getBlockType(),
                  new Tuple2<>(n.getLogBlockHeader(), n.getLogBlockFooter()), recordCount));
          totalEntries++;
        } else {
          List<Tuple3<HoodieLogBlockType, Tuple2<Map<HeaderMetadataType, String>, Map<HeaderMetadataType, String>>, Integer>> list
              = new ArrayList<>();
          list.add(new Tuple3<>(n.getBlockType(),
              new Tuple2<>(n.getLogBlockHeader(), n.getLogBlockFooter()), recordCount));
          commitCountAndMetadata.put(instantTime, list);
          totalEntries++;
        }
      }
    }
    String[][] rows = new String[totalEntries + 1][];
    int i = 0;
    ObjectMapper objectMapper = new ObjectMapper();
    for (Map.Entry<String, List<Tuple3<HoodieLogBlockType, Tuple2<Map<HeaderMetadataType, String>, Map<HeaderMetadataType, String>>, Integer>>> entry : commitCountAndMetadata
        .entrySet()) {
      String instantTime = entry.getKey().toString();
      for (Tuple3<HoodieLogBlockType, Tuple2<Map<HeaderMetadataType, String>, Map<HeaderMetadataType, String>>, Integer> tuple3 : entry
          .getValue()) {
        String[] output = new String[5];
        output[0] = instantTime;
        output[1] = String.valueOf(tuple3._3());
        output[2] = tuple3._1().toString();
        output[3] = objectMapper.writeValueAsString(tuple3._2()._1());
        output[4] = objectMapper.writeValueAsString(tuple3._2()._2());
        rows[i] = output;
        i++;
      }
    }
    return HoodiePrintHelper.print(
        new String[]{"InstantTime", "RecordCount", "BlockType", "HeaderMetadata", "FooterMetadata"},
        rows);
  }

  @CliCommand(value = "show logfile records", help = "Read records from log files")
  public String showLogFileRecords(
      @CliOption(key = {
          "limit"}, mandatory = false, help = "Limit commits", unspecifiedDefaultValue = "10")
      final Integer limit,
      @CliOption(key = "logFilePathPattern", mandatory = true, help = "Fully qualified paths for the log files")
      final String logFilePathPattern,
      @CliOption(key = "mergeRecords", mandatory = false, help = "If the records in the log files should be merged",
          unspecifiedDefaultValue = "false")
      final Boolean shouldMerge) throws IOException {

    System.out
        .println("===============> Showing only " + limit + " records <===============");

    FileSystem fs = HoodieCLI.tableMetadata.getFs();
    List<String> logFilePaths = Arrays.stream(fs.globStatus(new Path(logFilePathPattern)))
        .map(status -> status.getPath().toString()).collect(Collectors.toList());

    // TODO : readerSchema can change across blocks/log files, fix this inside Scanner
    AvroSchemaConverter converter = new AvroSchemaConverter();
    // get schema from last log file
    Schema readerSchema = converter
        .convert(SchemaUtil
            .readSchemaFromLogFile(fs, new Path(logFilePaths.get(logFilePaths.size() - 1))));

    List<IndexedRecord> allRecords = new ArrayList<>();

    if (shouldMerge) {
      System.out.println("===========================> MERGING RECORDS <===================");
      HoodieCompactedLogRecordScanner scanner = new HoodieCompactedLogRecordScanner(fs,
          HoodieCLI.tableMetadata.getBasePath(), logFilePaths, readerSchema,
          HoodieCLI.tableMetadata.getActiveTimeline().getCommitTimeline().lastInstant().get()
              .getTimestamp(),
          Long.valueOf(HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES),
          Boolean.valueOf(HoodieCompactionConfig.DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED),
          Boolean.valueOf(HoodieCompactionConfig.DEFAULT_COMPACTION_REVERSE_LOG_READ_ENABLED));
      for (HoodieRecord<? extends HoodieRecordPayload> hoodieRecord : scanner) {
        Optional<IndexedRecord> record = hoodieRecord.getData().getInsertValue(readerSchema);
        if (allRecords.size() >= limit) {
          break;
        }
        allRecords.add(record.get());
      }
    } else {
      for (String logFile : logFilePaths) {
        Schema writerSchema = new AvroSchemaConverter()
            .convert(SchemaUtil
                .readSchemaFromLogFile(HoodieCLI.tableMetadata.getFs(), new Path(logFile)));
        HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(fs,
            new HoodieLogFile(new Path(logFile)), writerSchema);
        // read the avro blocks
        while (reader.hasNext()) {
          HoodieLogBlock n = reader.next();
          if (n instanceof HoodieAvroDataBlock) {
            HoodieAvroDataBlock blk = (HoodieAvroDataBlock) n;
            List<IndexedRecord> records = blk.getRecords();
            allRecords.addAll(records);
            if (allRecords.size() >= limit) {
              break;
            }
          }
        }
        if (allRecords.size() >= limit) {
          break;
        }
      }
    }
    String[][] rows = new String[allRecords.size() + 1][];
    int i = 0;
    for (IndexedRecord record : allRecords) {
      String[] data = new String[1];
      data[0] = record.toString();
      rows[i] = data;
      i++;
    }
    return HoodiePrintHelper.print(
        new String[]{"Records"}, rows);
  }
}
