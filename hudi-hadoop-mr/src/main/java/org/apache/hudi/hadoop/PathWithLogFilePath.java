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

import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;

/**
 * we need to encode additional information in Path to track matching log file and base files.
 * Hence, this weird looking class which tracks an log/base file status
 */
public class PathWithLogFilePath extends Path {

  private boolean belongToIncrementalPath = false;
  private List<String> deltaLogPaths = new ArrayList<>();
  private String maxCommitTime = "";
  private String basePath = "";
  private String baseFilePath = "";

  public PathWithLogFilePath(Path parent, String child) {
    super(parent, child);
  }

  public boolean isBelongToIncrementalPath() {
    return belongToIncrementalPath;
  }

  public void setBelongToIncrementalPath(boolean belongToIncrementalPath) {
    this.belongToIncrementalPath = belongToIncrementalPath;
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

  public boolean splitable() {
    return !baseFilePath.isEmpty();
  }

  public BaseFileWithLogsSplit buildSplit(Path file, long start, long length, String[] hosts) {
    BaseFileWithLogsSplit bs = new BaseFileWithLogsSplit(file, start, length, hosts);
    bs.setBelongToIncrementalSplit(belongToIncrementalPath);
    bs.setDeltaLogPaths(deltaLogPaths);
    bs.setMaxCommitTime(maxCommitTime);
    bs.setBasePath(basePath);
    bs.setBaseFilePath(baseFilePath);
    return bs;
  }
}
