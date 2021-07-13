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

package org.apache.hudi.hadoop.realtime;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Iterator;

/**
 * Record Reader implementation to read avro data, to support inc queries.
 */
public class HoodieMergedLogReader extends AbstractRealtimeRecordReader
    implements RecordReader<NullWritable, ArrayWritable> {

  private final HoodieMergedLogRecordScanner logRecordScanner;
  private final Iterator<HoodieRecord<? extends HoodieRecordPayload>> logRecordsKeyIterator;
  private ArrayWritable valueObj;

  private int end;
  private int offset;

  public HoodieMergedLogReader(RealtimeSplit split, JobConf job, HoodieMergedLogRecordScanner logRecordScanner) {
    super(split, job);
    this.logRecordScanner = logRecordScanner;
    this.end = logRecordScanner.getRecords().size();
    this.logRecordsKeyIterator = logRecordScanner.iterator();
    this.valueObj = new ArrayWritable(Writable.class, new Writable[getHiveSchema().getFields().size()]);
  }

  private Option buildGenericRecordwithCustomPayload(HoodieRecord record) throws IOException {
    if (usesCustomPayload) {
      return record.getData().getInsertValue(getWriterSchema());
    } else {
      return record.getData().getInsertValue(getReaderSchema());
    }
  }

  @Override
  public boolean next(NullWritable key, ArrayWritable arrayWritable) throws IOException {
    if (!logRecordsKeyIterator.hasNext()) {
      return false;
    }
    Option<GenericRecord> rec;
    HoodieRecord currentRecord = logRecordsKeyIterator.next();
    rec = buildGenericRecordwithCustomPayload(currentRecord);
    // try to skip delete record
    while (!rec.isPresent() && logRecordsKeyIterator.hasNext()) {
      offset++;
      rec = buildGenericRecordwithCustomPayload(logRecordsKeyIterator.next());
    }
    if (!rec.isPresent()) {
      return false;
    }

    GenericRecord recordToReturn = rec.get();
    if (usesCustomPayload) {
      // If using a custom payload, return only the projection fields. The readerSchema is a schema derived from
      // the writerSchema with only the projection fields
      recordToReturn = HoodieAvroUtils.rewriteRecord(rec.get(), getReaderSchema());
    }
    ArrayWritable curWritable = (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(recordToReturn, getHiveSchema());
    if (arrayWritable != curWritable) {
      final Writable[] arrValue = arrayWritable.get();
      final Writable[] arrCurrent = curWritable.get();
      if (arrayWritable != null && arrValue.length == arrCurrent.length) {
        System.arraycopy(arrCurrent, 0, arrValue, 0, arrCurrent.length);
      } else {
        if (arrValue.length != arrCurrent.length) {
          throw new IOException(MessageFormat.format("HoodieMergeLogReader : size of object differs. Value size :  {0}, Current Object size : {1}",
              arrValue.length, arrCurrent.length));
        } else {
          throw new IOException("HoodieMergeLogReader can not support RecordReaders that don't return same key & value & value is null");
        }
      }
    }
    offset++;
    return true;
  }

  @Override
  public NullWritable createKey() {
    return null;
  }

  @Override
  public ArrayWritable createValue() {
    return valueObj;
  }

  @Override
  public long getPos() throws IOException {
    return offset;
  }

  @Override
  public void close() throws IOException {
    if (logRecordScanner != null) {
      logRecordScanner.close();
    }
  }

  @Override
  public float getProgress() throws IOException {
    if (end == offset) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (end - offset) / (float)(end));
    }
  }
}