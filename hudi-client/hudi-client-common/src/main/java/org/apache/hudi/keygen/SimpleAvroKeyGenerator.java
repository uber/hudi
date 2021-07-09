/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.keygen;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import java.util.Collections;
import java.util.List;

/**
 * Avro simple key generator, which takes names of fields to be used for recordKey and partitionPath as configs.
 */
public class SimpleAvroKeyGenerator extends BaseKeyGenerator {

  public SimpleAvroKeyGenerator(TypedProperties props) {
    this(props, props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY.key()),
        props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY.key()),
        props.getString(KeyGeneratorOptions.INDEXKEY_FILED_OPT.key(),
            KeyGeneratorOptions.INDEXKEY_FILED_OPT.defaultValue()));
  }

  SimpleAvroKeyGenerator(TypedProperties props, String partitionPathField) {
    this(props, null, partitionPathField, null);
  }

  SimpleAvroKeyGenerator(TypedProperties props, String recordKeyField, String partitionPathField) {
    this(props, recordKeyField, partitionPathField, null);
  }

  SimpleAvroKeyGenerator(TypedProperties props, String recordKeyField, String partitionPathField,
      String indexKeyField) {
    super(props);
    this.recordKeyFields = recordKeyField == null
        ? Collections.emptyList()
        : Collections.singletonList(recordKeyField);
    this.partitionPathFields = Collections.singletonList(partitionPathField);
    indexKeyField = StringUtils.isNullOrEmpty(indexKeyField) ? props
        .getString(KeyGeneratorOptions.INDEXKEY_FILED_OPT.key(),
            KeyGeneratorOptions.INDEXKEY_FILED_OPT.defaultValue()) : indexKeyField;
    this.indexKeyFields = StringUtils.isNullOrEmpty(indexKeyField)
        ? Collections.emptyList()
        : Collections.singletonList(indexKeyField);
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return KeyGenUtils.getRecordKey(record, getRecordKeyFields().get(0));
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return KeyGenUtils.getPartitionPath(record, getPartitionPathFields().get(0), hiveStylePartitioning, encodePartitionPath);
  }

  @Override
  public List<Object> getIndexKey(GenericRecord record) {
    return KeyGenUtils.getIndexKey(record, getIndexKeyFields());
  }
}
