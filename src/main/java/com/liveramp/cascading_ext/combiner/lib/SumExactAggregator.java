/**
 * Copyright 2012 LiveRamp
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.liveramp.cascading_ext.combiner.lib;

import java.io.Serializable;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.liveramp.cascading_ext.combiner.ExactAggregator;
import com.liveramp.commons.util.IntegerMemoryUsageEstimator;
import com.liveramp.commons.util.LongMemoryUsageEstimator;
import com.liveramp.commons.util.MemoryUsageEstimator;

public class SumExactAggregator extends BaseExactAggregator<Number[]> implements ExactAggregator<Number[]> {

  private final int numOutputFields;
  private transient Mode mode;

  private enum Mode {
    LONG, DOUBLE
  }

  public SumExactAggregator(int numOutputFields) {
    this.numOutputFields = numOutputFields;
  }

  @Override
  public Number[] initialize() {
    return new Number[numOutputFields];
  }

  @Override
  public Number[] partialAggregate(Number[] aggregate, TupleEntry nextValue) {
    for (int i = 0; i < aggregate.length; i++) {
      // Determine mode if necessary
      if (mode == null) {
        Object value = nextValue.getObject(i);
        if (value instanceof Long || value instanceof Integer) {
          mode = Mode.LONG;
        } else if (value instanceof Double || value instanceof Float) {
          mode = Mode.DOUBLE;
        } else {
          throw new RuntimeException("Unknown number class:" + value.getClass());
        }
      }
      switch (mode) {
        case LONG:
          if (aggregate[i] == null) {
            aggregate[i] = 0L;
          }
          aggregate[i] = ((Long)aggregate[i]) + nextValue.getLong(i);
          break;
        case DOUBLE:
          if (aggregate[i] == null) {
            aggregate[i] = 0D;
          }
          aggregate[i] = ((Double)aggregate[i]) + nextValue.getDouble(i);
          break;
      }
    }
    return aggregate;
  }

  @Override
  public Number[] finalAggregate(Number[] aggregate, TupleEntry partialAggregate) {
    return partialAggregate(aggregate, partialAggregate);
  }

  @Override
  public Tuple toTuple(Number[] values) {
    Tuple tuple = new Tuple();
    for (Number value : values) {
      tuple.add(value);
    }
    return tuple;
  }

  public static class SumExactAggregatorMemoryUsageEstimator implements MemoryUsageEstimator<Number[]>, Serializable {

    private static final IntegerMemoryUsageEstimator imue = new IntegerMemoryUsageEstimator();
    private static final LongMemoryUsageEstimator lmue = new LongMemoryUsageEstimator();

    @Override
    public long estimateMemorySize(Number[] items) {
      long size = 12L;  // estimate of array overhead

      for (Number number : items) {
        if (number == null) {
          size += 8L;
        } else if (number instanceof Integer) {
          size += imue.estimateMemorySize((Integer)number);
        } else if (number instanceof Long) {
          size += lmue.estimateMemorySize((Long)number);
        } else if (number instanceof Double) {
          size += 16L;  // same as Long
        } else if (number instanceof Short) {
          size += 8L;
        } else if (number instanceof Float) {
          size += 16L;  // same as Integer
        } else if (number instanceof Byte) {
          size += 8L;
        } else {
          throw new RuntimeException("Unrecognized number format: " + number.getClass());
        }
      }
      return size;
    }
  }
}
