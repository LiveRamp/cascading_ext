package com.liveramp.cascading_ext.combiner;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

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
          aggregate[i] = ((Long) aggregate[i]) + nextValue.getLong(i);
          break;
        case DOUBLE:
          if (aggregate[i] == null) {
            aggregate[i] = 0D;
          }
          aggregate[i] = ((Double) aggregate[i]) + nextValue.getDouble(i);
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
}
