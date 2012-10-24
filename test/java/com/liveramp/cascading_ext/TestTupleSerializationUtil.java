package com.liveramp.cascading_ext;

import cascading.tuple.Tuple;
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author eddie
 */
public class TestTupleSerializationUtil extends BaseTestCase {
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

  public void testSerializationWithNativeObjects() throws Exception {
    Tuple tuple = new Tuple(1, "andre", 123L);

    TupleSerializationUtil serializationUtil = new TupleSerializationUtil(CascadingUtil.get().getJobConf());
    byte[] serializedTuple = serializationUtil.serialize(tuple);

    Tuple expectedTuple = new Tuple(1, "andre", 123L);
    Tuple actualTuple = serializationUtil.deserialize(serializedTuple);

    assertEquals(expectedTuple, actualTuple);
  }

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
