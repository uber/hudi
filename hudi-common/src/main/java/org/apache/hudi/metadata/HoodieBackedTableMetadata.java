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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Table metadata provided by an internal DFS backed Hudi metadata table.
 *
 * If the metadata table does not exist, RPC calls are used to retrieve file listings from the file system.
 * No updates are applied to the table and it is not synced.
 */
public class HoodieBackedTableMetadata extends BaseTableMetadata {

  private static final Logger LOG = LogManager.getLogger(HoodieBackedTableMetadata.class);

  private String metadataBasePath;
  // Metadata table's timeline and metaclient
  private HoodieTableMetaClient metaClient;
  private List<FileSlice> latestFileSystemMetadataSlices;

  // Readers for the base and log file which store the metadata
  private transient HoodieFileReader<GenericRecord> baseFileReader;
  private transient HoodieMetadataMergedLogRecordScanner logRecordScanner;

  public HoodieBackedTableMetadata(Configuration conf, HoodieMetadataConfig metadataConfig,
                                   String datasetBasePath, String spillableMapDirectory) {
    this(new HoodieLocalEngineContext(conf), metadataConfig, datasetBasePath, spillableMapDirectory);
  }

  public HoodieBackedTableMetadata(HoodieEngineContext engineContext, HoodieMetadataConfig metadataConfig,
                                   String datasetBasePath, String spillableMapDirectory) {
    super(engineContext, metadataConfig, datasetBasePath, spillableMapDirectory);
    initIfNeeded();
  }

  private void initIfNeeded() {
    if (enabled && this.metaClient == null) {
      this.metadataBasePath = HoodieTableMetadata.getMetadataTableBasePath(datasetBasePath);
      try {
        this.metaClient = new HoodieTableMetaClient(hadoopConf.get(), metadataBasePath);
        HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline());
        latestFileSystemMetadataSlices = fsView.getLatestFileSlices(MetadataPartitionType.FILES.partitionPath()).collect(Collectors.toList());
      } catch (TableNotFoundException e) {
        LOG.warn("Metadata table was not found at path " + metadataBasePath);
        this.enabled = false;
        this.metaClient = null;
      } catch (Exception e) {
        LOG.error("Failed to initialize metadata table at path " + metadataBasePath, e);
        this.enabled = false;
        this.metaClient = null;
      }
    } else {
      LOG.info("Metadata table is disabled.");
    }
  }

  @Override
  protected Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKeyFromMetadata(String key) {
    try {
      //TODO(metadata): remove the timers or turn them into good log statements.
      HoodieTimer timer = new HoodieTimer().startTimer();
      openFileSliceIfNeeded();

      System.err.println(" >> Open took " + timer.endTimer());

      timer.startTimer();
      // Retrieve record from base file
      HoodieRecord<HoodieMetadataPayload> hoodieRecord = null;
      if (baseFileReader != null) {
        HoodieTimer readTimer = new HoodieTimer().startTimer();
        Option<GenericRecord> baseRecord = baseFileReader.getRecordByKey(key);
        if (baseRecord.isPresent()) {
          hoodieRecord = SpillableMapUtils.convertToHoodieRecordPayload(baseRecord.get(),
              metaClient.getTableConfig().getPayloadClass());
          metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.BASEFILE_READ_STR, readTimer.endTimer()));
        }
      }
      System.err.println(" >> Read of base file took " + timer.endTimer());

      // Retrieve record from log file
      timer.startTimer();
      if (logRecordScanner != null) {
        Option<HoodieRecord<HoodieMetadataPayload>> logHoodieRecord = logRecordScanner.getRecordByKey(key);
        if (logHoodieRecord.isPresent()) {
          if (hoodieRecord != null) {
            // Merge the payloads
            HoodieRecordPayload mergedPayload = logHoodieRecord.get().getData().preCombine(hoodieRecord.getData());
            hoodieRecord = new HoodieRecord(hoodieRecord.getKey(), mergedPayload);
          } else {
            hoodieRecord = logHoodieRecord.get();
          }
        }
      }
      System.err.println(" >> Read of merged/log file took " + timer.endTimer());
      return Option.ofNullable(hoodieRecord);
    } catch (IOException ioe) {
      throw new HoodieIOException("Error merging records from metadata table for key :" + key, ioe);
    } finally {
      HoodieTimer timer = new HoodieTimer().startTimer();
      closeIfNeeded();
      System.err.println(" >> Close took " + timer.endTimer());
    }
  }

  /**
   * Open readers to the base and log files.
   */
  private synchronized void openFileSliceIfNeeded() throws IOException {
    if (metadataConfig.enableReuse() && baseFileReader != null) {
      // we will reuse what's open.
      return;
    }

    // Metadata is in sync till the latest completed instant on the dataset
    HoodieTimer timer = new HoodieTimer().startTimer();
    String latestInstantTime = getLatestDatasetInstantTime();
    ValidationUtils.checkArgument(latestFileSystemMetadataSlices.size() == 1, "must be at-least one validata metadata file slice");

    // If the base file is present then create a reader
    Option<HoodieBaseFile> basefile = latestFileSystemMetadataSlices.get(0).getBaseFile();
    if (basefile.isPresent()) {
      String basefilePath = basefile.get().getPath();
      baseFileReader = HoodieFileReaderFactory.getFileReader(hadoopConf.get(), new Path(basefilePath));
      LOG.info("Opened metadata base file from " + basefilePath + " at instant " + basefile.get().getCommitTime());
    }

    // Open the log record scanner using the log files from the latest file slice
    List<String> logFilePaths = latestFileSystemMetadataSlices.get(0).getLogFiles()
        .sorted(HoodieLogFile.getLogFileComparator())
        .map(o -> o.getPath().toString())
        .collect(Collectors.toList());
    Option<HoodieInstant> lastInstant = metaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
    String latestMetaInstantTimestamp = lastInstant.map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);

    // Load the schema
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());
    logRecordScanner = new HoodieMetadataMergedLogRecordScanner(metaClient.getFs(), metadataBasePath,
            logFilePaths, schema, latestMetaInstantTimestamp, MAX_MEMORY_SIZE_IN_BYTES, BUFFER_SIZE,
            spillableMapDirectory, null);

    LOG.info("Opened metadata log files from " + logFilePaths + " at instant " + latestInstantTime
        + "(dataset instant=" + latestInstantTime + ", metadata instant=" + latestMetaInstantTimestamp + ")");

    metrics.ifPresent(metrics -> metrics.updateMetrics(HoodieMetadataMetrics.SCAN_STR, timer.endTimer()));
  }

  private void closeIfNeeded() {
    try {
      if (!metadataConfig.enableReuse()) {
        close();
      }
    } catch (Exception e) {
      throw new HoodieException("Error closing resources during metadata table merge", e);
    }
  }

  @Override
  public void close() throws Exception {
    if (baseFileReader != null) {
      baseFileReader.close();
      baseFileReader = null;
    }
    if (logRecordScanner != null) {
      logRecordScanner.close();
      logRecordScanner = null;
    }
    //TODO(metadata): remove all this
    /*    try {
      throw new HoodieException("Metadata closed from here");
    } catch (Exception e) {
      LOG.warn("Metadata closed", e);
    }*/
  }

  /**
   * Return an ordered list of instants which have not been synced to the Metadata Table.
   */
  protected List<HoodieInstant> findInstantsToSync() {
    initIfNeeded();

    // if there are no instants yet, return empty list, since there is nothing to sync here.
    if (!enabled || !metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().isPresent()) {
      return Collections.EMPTY_LIST;
    }

    // All instants on the data timeline, which are greater than the last instant on metadata timeline
    // are candidates for sync.
    String latestMetadataInstantTime = metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().get().getTimestamp();
    HoodieDefaultTimeline candidateTimeline = datasetMetaClient.getActiveTimeline().findInstantsAfter(latestMetadataInstantTime, Integer.MAX_VALUE);
    Option<HoodieInstant> earliestIncompleteInstant = candidateTimeline.filterInflightsAndRequested().firstInstant();

    if (earliestIncompleteInstant.isPresent()) {
      return candidateTimeline.filterCompletedInstants()
          .findInstantsBefore(earliestIncompleteInstant.get().getTimestamp())
          .getInstants().collect(Collectors.toList());
    } else {
      return candidateTimeline.filterCompletedInstants()
          .getInstants().collect(Collectors.toList());
    }
  }

  /**
   * Return the timestamp of the latest compaction instant.
   */
  @Override
  public Option<String> getSyncedInstantTime() {
    if (!enabled) {
      return Option.empty();
    }

    HoodieActiveTimeline timeline = metaClient.reloadActiveTimeline();
    return timeline.getDeltaCommitTimeline().filterCompletedInstants()
        .lastInstant().map(HoodieInstant::getTimestamp);
  }

  public boolean enabled() {
    return enabled;
  }

  public SerializableConfiguration getHadoopConf() {
    return hadoopConf;
  }

  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }

  public Map<String, String> stats() {
    return metrics.map(m -> m.getStats(true, metaClient, this)).orElse(new HashMap<>());
  }
}
