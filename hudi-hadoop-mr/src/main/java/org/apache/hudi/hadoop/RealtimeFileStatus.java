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

package org.apache.hudi.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RealtimeFileStatus extends FileStatus {
  private boolean belongToIncrementalFileStatus = false;
  private boolean logFileOnly = false;
  private List<String> deltaLogPaths = new ArrayList<>();
  private String maxCommitTime = "";
  private String basePath = "";
  private String baseFilePath = "";

  public RealtimeFileStatus(FileStatus fileStatus) throws IOException {
    super(fileStatus);
  }

  @Override
  public Path getPath() {
    Path path = super.getPath();
    PathWithLogFilePath pathWithLogFilePath = new PathWithLogFilePath(path.getParent(), path.getName());
    pathWithLogFilePath.setBelongToIncrementalPath(belongToIncrementalFileStatus);
    pathWithLogFilePath.setDeltaLogPaths(deltaLogPaths);
    pathWithLogFilePath.setMaxCommitTime(maxCommitTime);
    pathWithLogFilePath.setBasePath(basePath);
    pathWithLogFilePath.setBaseFilePath(baseFilePath);
    return pathWithLogFilePath;
  }

  public boolean isBelongToIncrementalFileStatus() {
    return belongToIncrementalFileStatus;
  }

  public void setBelongToIncrementalFileStatus(boolean belongToIncrementalFileStatus) {
    this.belongToIncrementalFileStatus = belongToIncrementalFileStatus;
  }

  public boolean isLogFileOnly() {
    return logFileOnly;
  }

  public void setLogFileOnly(boolean logFileOnly) {
    this.logFileOnly = logFileOnly;
  }

  public List<String> getDeltaLogPaths() {
    return deltaLogPaths;
  }

  public void setDeltaLogPaths(List<String> deltaLogPaths) {
    this.deltaLogPaths = deltaLogPaths;
  }

  public String getMaxCommitTime() {
    return maxCommitTime;
  }

  public void setMaxCommitTime(String maxCommitTime) {
    this.maxCommitTime = maxCommitTime;
  }

  public String getBasePath() {
    return basePath;
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  public String getBaseFilePath() {
    return baseFilePath;
  }

  public void setBaseFilePath(String baseFilePath) {
    this.baseFilePath = baseFilePath;
  }
}
