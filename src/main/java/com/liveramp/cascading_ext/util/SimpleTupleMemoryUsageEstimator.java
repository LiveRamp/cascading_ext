package com.liveramp.cascading_ext.util;

import cascading.tuple.Tuple;
import com.liveramp.commons.util.IntegerMemoryUsageEstimator;
import com.liveramp.commons.util.LongMemoryUsageEstimator;
import com.liveramp.commons.util.MemoryUsageEstimator;
import com.liveramp.commons.util.StringMemoryUsageEstimator;
import org.apache.hadoop.io.BytesWritable;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Estimate memory usage of a Tuple containing simple objects.
 */
public class SimpleTupleMemoryUsageEstimator implements MemoryUsageEstimator<Tuple>, Serializable {

  private static final StringMemoryUsageEstimator smue = new StringMemoryUsageEstimator();
  private static final IntegerMemoryUsageEstimator imue = new IntegerMemoryUsageEstimator();
  private static final LongMemoryUsageEstimator lmue = new LongMemoryUsageEstimator();

  @Override
  public long estimateMemorySize(Tuple tuple) {
    long size = 20; // rough estimate of Tuple overhead

    for (int i = 0; i < tuple.size(); i++) {
      Object obj = tuple.getObject(i);
      if (obj == null) {
        size += 8;
      } else if (obj instanceof Integer) {
        size += imue.estimateMemorySize((Integer) obj);
      } else if (obj instanceof String) {
        size += smue.estimateMemorySize((String) obj);
      } else if (obj instanceof Long) {
        size += lmue.estimateMemorySize((Long) obj);
      } else if (obj instanceof ByteBuffer) {
        size += 8 + ((ByteBuffer) obj).capacity();
      } else if (obj instanceof BytesWritable) {
        size += 8 + ((BytesWritable) obj).getCapacity();
      } else {
        throw new RuntimeException("Unrecognized tuple element: " + obj.getClass());
      }
    }

    return size;
  }
}
