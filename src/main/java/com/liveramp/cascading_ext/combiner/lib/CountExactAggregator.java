package com.liveramp.cascading_ext.combiner.lib;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.liveramp.cascading_ext.combiner.ExactAggregator;
import com.liveramp.cascading_ext.combiner.lib.BaseExactAggregator;

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
