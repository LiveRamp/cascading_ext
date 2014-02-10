package com.liveramp.cascading_ext.combiner;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class CountExactAggregator extends BaseExactAggregator<Long> implements ExactAggregator<Long> {

  @Override
  public Long initialize() {
    return 0L;
  }

  @Override
  public Long partialAggregate(Long aggregate, TupleEntry nextValue) {
      return ++aggregate;
    }

  @Override
  public Long finalAggregate(Long aggregate, TupleEntry partialAggregate) {
    return aggregate += partialAggregate.getLong(0);
  }

  @Override
  public Tuple toTuple(Long value) {
    return new Tuple(value);
  }
}
