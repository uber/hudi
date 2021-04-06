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

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.commands.SparkMain.SparkCommand;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;
import scala.collection.JavaConverters;

@Component
public class ClusteringCommand implements CommandMarker {

  private static final Logger LOG = LogManager.getLogger(ClusteringCommand.class);

  @CliCommand(value = "clustering schedule", help = "Schedule Clustering")
  public String scheduleClustering(
      @CliOption(key = "sparkMemory", help = "Spark executor memory",
          unspecifiedDefaultValue = "1G") final String sparkMemory,
      @CliOption(key = "propsFilePath", help = "path to properties file on localfs or dfs with configurations for hoodie client for clustering",
          unspecifiedDefaultValue = "") final String propsFilePath,
      @CliOption(key = "hoodieConfigs", help = "Any configuration that can be set in the properties file can be passed here in the form of an array",
          unspecifiedDefaultValue = "") final String[] configs) throws Exception {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    String sparkPropertiesPath =
        Utils.getDefaultPropertiesFile(JavaConverters.mapAsScalaMapConverter(System.getenv()).asScala());
    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.setMaster("local[4]");

    // First get a clustering instant time and pass it to spark launcher for scheduling clustering
    String clusteringInstantTime = HoodieActiveTimeline.createNewInstantTime();

    sparkLauncher.addAppArgs(SparkCommand.CLUSTERING_SCHEDULE.toString(), client.getBasePath(),
        client.getTableConfig().getTableName(), clusteringInstantTime, sparkMemory, propsFilePath);
    UtilHelpers.validateAndAddProperties(configs, sparkLauncher);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      return "Failed to schedule clustering for " + clusteringInstantTime;
    }
    return "Attempted to schedule clustering for " + clusteringInstantTime;
  }

  @CliCommand(value = "clustering run", help = "Run Clustering")
  public String runClustering(
      @CliOption(key = "parallelism", mandatory = true,
          help = "Parallelism for hoodie clustering") final String parallelism,
      @CliOption(key = "sparkMemory", unspecifiedDefaultValue = "4G",
          help = "Spark executor memory") final String sparkMemory,
      @CliOption(key = "retry", unspecifiedDefaultValue = "1", help = "Number of retries") final String retry,
      @CliOption(key = "clusteringInstant", help = "Base path for the target hoodie table") final String clusteringInstantTime,
      @CliOption(key = "propsFilePath", help = "path to properties file on localfs or dfs with configurations for hoodie client for compacting",
          unspecifiedDefaultValue = "") final String propsFilePath,
      @CliOption(key = "hoodieConfigs", help = "Any configuration that can be set in the properties file can be passed here in the form of an array",
          unspecifiedDefaultValue = "") final String[] configs
  ) throws Exception {
    HoodieTableMetaClient client = HoodieCLI.getTableMetaClient();
    String sparkPropertiesPath =
        Utils.getDefaultPropertiesFile(JavaConverters.mapAsScalaMapConverter(System.getenv()).asScala());
    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);
    sparkLauncher.addAppArgs(SparkCommand.CLUSTERING_RUN.toString(), client.getBasePath(),
        client.getTableConfig().getTableName(), clusteringInstantTime, parallelism, sparkMemory, retry, propsFilePath);
    UtilHelpers.validateAndAddProperties(configs, sparkLauncher);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      return "Failed to run clustering for " + clusteringInstantTime;
    }
    return "Clustering successfully completed for " + clusteringInstantTime;

  }
}
