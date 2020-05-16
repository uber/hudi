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

package org.apache.hudi.utilities.schema;

import org.apache.avro.Schema;
import org.apache.hudi.common.config.TypedProperties;

/**
 * Schema provider to be used in HoodieAvroKafkaDeserializer test cases.
 */
public class DummySchemaProvider extends SchemaProvider {

  public DummySchemaProvider(TypedProperties properties) {
    super(properties, null);
  }

  public Schema getSourceSchema() {
    return new Schema.Parser().parse("{\"namespace\":\"example.avro\",\"type\":\"record\",\"name\":\"User\",\"fields\":"
        + "[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":\"int\"},{\"name\":\"favorite_color\",\"type\":\"string\"}]}");
  }
}
