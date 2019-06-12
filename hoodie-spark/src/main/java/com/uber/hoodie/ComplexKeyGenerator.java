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

package com.uber.hoodie;


/**
 * Complex key generator, which takes names of fields to be used for recordKey and partitionPath as
 * configs.
 */
import com.uber.hoodie.DataSourceUtils;
import com.uber.hoodie.DataSourceWriteOptions;
import com.uber.hoodie.KeyGenerator;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.exception.HoodieException;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import com.google.gson.*;

public class ComplexKeyGenerator extends KeyGenerator {

  private static final String DEFAULT_PARTITION_PATH = "default";

  private static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";

  protected final List<String> recordKeyFields;

  protected final List<String> partitionPathFields;

  public ComplexKeyGenerator(TypedProperties props) {
    super(props);
    this.recordKeyFields = Arrays.asList(props.getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY()).split(","));
    this.partitionPathFields = Arrays.asList(props
            .getString(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY()).split(","));
  }

  @Override
  public HoodieKey getKey(GenericRecord record) {
    if (recordKeyFields == null || partitionPathFields == null) {
      throw new HoodieException(
              "Unable to find field names for record key or partition path in cfg");
    }

    JsonObject recordKeyJson = new JsonObject();


    for (String recordKeyField : recordKeyFields) {
      recordKeyJson.addProperty(recordKeyField,DataSourceUtils.getNestedFieldValAsString(record, recordKeyField));
    }
    Gson gson = new Gson();
    String recordKey = gson.toJson(recordKeyJson);
    StringBuilder partitionPath = new StringBuilder();
    try {
      for (String partitionPathField : partitionPathFields) {
        partitionPath.append(DataSourceUtils.getNestedFieldValAsString(record, partitionPathField));
        partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
      }
      partitionPath.delete(partitionPath.length() - 1, partitionPath.length());
    } catch (HoodieException e) {
      // TODO : optimize this since throwing and catching exception is cpu intensive
      // if field is not found, lump it into default partition
      partitionPath = partitionPath.append(DEFAULT_PARTITION_PATH);
    }

    return new HoodieKey(recordKey.toString(), partitionPath.toString());
  }

  public List<String> getRecordKeyFields() {
    return recordKeyFields;
  }

  public List<String> getPartitionPathFields() {
    return partitionPathFields;
  }
}