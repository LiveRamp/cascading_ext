package com.liveramp.cascading_ext.combiner.lib;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.liveramp.cascading_ext.combiner.ExactAggregator;

public abstract class MxxExactAggregator extends BaseExactAggregator<Long> implements ExactAggregator<Long> {

  @Override
  public Tuple toTuple(Long aggregate) {
    return new Tuple(aggregate);
  }

  @Override
  public Long partialAggregate(Long aggregate, TupleEntry nextValue) {
    return update(aggregate, nextValue.getLong(0));
  }

  @Override
  public Long finalAggregate(Long aggregate, TupleEntry partialAggregate) {
    return update(aggregate, partialAggregate.getLong(0));
  }

  protected abstract Long update(Long aggregate, Long nextValue);

}
