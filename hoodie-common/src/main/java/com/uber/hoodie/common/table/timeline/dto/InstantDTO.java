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

package com.uber.hoodie.common.table.timeline.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.uber.hoodie.common.table.timeline.HoodieInstant;


@JsonIgnoreProperties(ignoreUnknown = true)
public class InstantDTO {

  @JsonProperty("action")
  String action;
  @JsonProperty("ts")
  String timestamp;
  @JsonProperty("state")
  String state;

  public static InstantDTO fromInstant(HoodieInstant instant) {
    if (null == instant) {
      return null;
    }

    InstantDTO dto = new InstantDTO();
    dto.action = instant.getAction();
    dto.timestamp = instant.getTimestamp();
    dto.state = instant.getState().toString();
    return dto;
  }

  public static HoodieInstant toInstant(InstantDTO dto) {
    if (null == dto) {
      return null;
    }

    return new HoodieInstant(HoodieInstant.State.valueOf(dto.state), dto.action, dto.timestamp);
  }
}
