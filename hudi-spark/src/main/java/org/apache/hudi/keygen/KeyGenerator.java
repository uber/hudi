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

package org.apache.hudi.keygen;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.AvroConversionHelper;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import scala.Function1;

import java.io.Serializable;
import java.util.List;

/**
 * Abstract class to extend for plugging in extraction of {@link HoodieKey} from an Avro record.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public abstract class KeyGenerator implements Serializable, KeyGeneratorInterface {

  private static final String STRUCT_NAME = "hoodieRowTopLevelField";
  private static final String NAMESPACE = "hoodieRow";

  protected transient TypedProperties config;
  private transient Function1<Object, Object> converterFn = null;

  protected KeyGenerator(TypedProperties config) {
    this.config = config;
  }

  /**
   * Generate a Hoodie Key out of provided generic record.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  public abstract HoodieKey getKey(GenericRecord record);

  /**
   * Used during bootstrap, to project out only the record key fields from bootstrap source dataset.
   *
   * @return list of field names, when concatenated make up the record key.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public List<String> getRecordKeyFieldNames() {
    throw new UnsupportedOperationException("Bootstrap not supported for key generator. "
        + "Please override this method in your custom key generator.");
  }

  /**
   * Fetch record key from {@link Row}.
   * @param row instance of {@link Row} from which record key is requested.
   * @return the record key of interest from {@link Row}.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public String getRecordKey(Row row) {
    if (null == converterFn) {
      converterFn = AvroConversionHelper.createConverterToAvro(row.schema(), STRUCT_NAME, NAMESPACE);
    }
    GenericRecord genericRecord = (GenericRecord) converterFn.apply(row);
    return getKey(genericRecord).getRecordKey();
  }

  /**
   * Fetch partition path from {@link Row}.
   * @param row instance of {@link Row} from which partition path is requested
   * @return the partition path of interest from {@link Row}.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public String getPartitionPath(Row row) {
    if (null == converterFn) {
      converterFn = AvroConversionHelper.createConverterToAvro(row.schema(), STRUCT_NAME, NAMESPACE);
    }
    GenericRecord genericRecord = (GenericRecord) converterFn.apply(row);
    return getKey(genericRecord).getPartitionPath();
  }
}
