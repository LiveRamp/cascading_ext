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
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

public class TestTupleSerializationUtil extends BaseTestCase {

  @Test
  public void testSerializationWithBytesWritables() throws Exception {
    BytesWritable bw1 = new BytesWritable(new byte[]{1, 2, 3, 4});
    BytesWritable bw2 = new BytesWritable(new byte[]{1, 2, 8, 9});
    BytesWritable bw3 = new BytesWritable(new byte[]{1, 1, 1, 2});
    Tuple tuple = new Tuple(bw1, bw2, bw3);

    TupleSerializationUtil serializationUtil = new TupleSerializationUtil(CascadingUtil.get().getJobConf());
    byte[] serializedTuple = serializationUtil.serialize(tuple);

    Tuple expectedTuple = new Tuple(bw1, bw2, bw3);
    Tuple actualTuple = serializationUtil.deserialize(serializedTuple);

    assertEquals(expectedTuple, actualTuple);
  }

  @Test
  public void testSerializationWithNativeObjects() throws Exception {
    Tuple tuple = new Tuple(1, "andre", 123L);

    TupleSerializationUtil serializationUtil = new TupleSerializationUtil(CascadingUtil.get().getJobConf());
    byte[] serializedTuple = serializationUtil.serialize(tuple);

    Tuple expectedTuple = new Tuple(1, "andre", 123L);
    Tuple actualTuple = serializationUtil.deserialize(serializedTuple);

    assertEquals(expectedTuple, actualTuple);
  }

  @Test
  public void testNoErrorsAcrossSerializations() throws IOException {
    TupleSerializationUtil serializationUtil = new TupleSerializationUtil(CascadingUtil.get().getJobConf());
    List<Tuple> inputTuples = new ArrayList<Tuple>();
    List<byte[]> intermediateSerializations = new ArrayList<byte[]>();
    int limit = 30;

    for (int i = 0; i < limit; i++) {
      Tuple tuple = new Tuple(UUID.randomUUID().toString(), i);
      inputTuples.add(tuple);
      intermediateSerializations.add(serializationUtil.serialize(tuple));
    }

    for (int i = 0; i < limit; i++) {
      byte[] serTuple = intermediateSerializations.get(i);
      assertEquals("Checking equality for tuple # " + i, inputTuples.get(i), serializationUtil.deserialize(serTuple));
    }
  }
}
