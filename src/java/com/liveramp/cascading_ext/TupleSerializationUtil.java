/**
 *  Copyright 2012 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.liveramp.cascading_ext;

import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.io.HadoopTupleInputStream;
import cascading.tuple.hadoop.io.HadoopTupleOutputStream;
import cascading.tuple.io.TupleInputStream;
import cascading.tuple.io.TupleOutputStream;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.JobConf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

public class TupleSerializationUtil implements Serializable {
  private static final int BUFFER_SIZE = 4096;
  private final JobConf jobConf;
  private transient TupleSerialization serialization = null;
  private transient ByteArrayOutputStream bytesOutputStream = null;
  private transient TupleOutputStream tupleOutputStream = null;
  private transient Serializer<Tuple> tupleSerializer = null;
  private transient Deserializer<Tuple> tupleDeserializer = null;

  public TupleSerializationUtil(JobConf jobConf) {
    this.jobConf = jobConf;
  }

  public byte[] serialize(Tuple tuple) throws IOException {
    initSerializer();
    bytesOutputStream.reset();
    tupleSerializer.open(tupleOutputStream);
    tupleSerializer.serialize(tuple);
    return bytesOutputStream.toByteArray();
  }

  public Tuple deserialize(byte[] bytes) throws IOException {
    initDeserializer();
    Tuple tuple = new Tuple();
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    TupleInputStream tupleInputStream = new HadoopTupleInputStream(inputStream, serialization.getElementReader());
    tupleDeserializer.open(tupleInputStream);
    tupleDeserializer.deserialize(tuple);
    return tuple;
  }

  private void initSerializer(){
    init();
    if (bytesOutputStream == null) {
      bytesOutputStream = new ByteArrayOutputStream(BUFFER_SIZE);
    }
    if (tupleOutputStream == null) {
      tupleOutputStream = new HadoopTupleOutputStream(bytesOutputStream, serialization.getElementWriter());
    }
    if (tupleSerializer == null) {
      tupleSerializer = serialization.getSerializer(Tuple.class);
    }
  }

  private void initDeserializer(){
    init();
    if (tupleDeserializer == null) {
      tupleDeserializer = serialization.getDeserializer(Tuple.class);
    }
  }

  private void init() {
    if (serialization == null) {
      serialization = new TupleSerialization(jobConf);
    }
  }
}
