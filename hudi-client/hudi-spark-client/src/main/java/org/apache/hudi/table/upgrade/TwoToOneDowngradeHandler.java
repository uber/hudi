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

package org.apache.hudi.table.upgrade;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.table.HoodieSparkTable;

/**
 * Downgrade handle to assist in downgrading hoodie table from version 2 to 1.
 */
public  class TwoToOneDowngradeHandler implements DowngradeHandler {

  @Override
  public void downgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime) {
    // fetch pending commit info
    HoodieSparkTable table = HoodieSparkTable.create(config, context);
    HoodieTimeline versionTwoActiveTimeline = HoodieTableMetaClient.builder().setConf(table.getMetaClient().getHadoopConf()).setBasePath(config.getBasePath())
        .setLoadActiveTimelineOnLoad(true).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(TimelineLayoutVersion.VERSION_2))).build().getActiveTimeline();
    HoodieTimeline versionOneActiveTimeline = HoodieTableMetaClient.builder().setConf(table.getMetaClient().getHadoopConf()).setBasePath(config.getBasePath())
        .setLoadActiveTimelineOnLoad(true).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
        .setLayoutVersion(Option.of(new TimelineLayoutVersion(TimelineLayoutVersion.VERSION_1))).build().getActiveTimeline();
    List<HoodieInstant> versionTwoInstants = versionTwoActiveTimeline.getInstants().collect(Collectors.toList());
    List<HoodieInstant> versionOneInstants = versionOneActiveTimeline.getInstants().collect(Collectors.toList());
    for (HoodieInstant instant : versionTwoInstants) {
      // delete existing marker files
      String versionOneFileName = instant.getFileName();
      String versionTwoFileName = versionOneInstants.stream().filter(s -> s.getTimestamp()
          .equals(instant.getTimestamp())).findFirst().get().getFileName();
      String metaPath = table.getMetaClient().getMetaPath();
      // rename current to previous or create new and delete old
      try {
        table.getMetaClient().getFs().rename(new Path(metaPath, versionTwoFileName), new Path(metaPath, versionOneFileName));
      } catch (IOException io) {
        throw new HoodieUpgradeDowngradeException("Unable to rename instant filename ", io);
      }
    }
  }
}
