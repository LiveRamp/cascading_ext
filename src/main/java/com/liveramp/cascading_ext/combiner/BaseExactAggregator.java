package com.liveramp.cascading_ext.combiner;

import cascading.tuple.Tuple;

public abstract class BaseExactAggregator<T> implements ExactAggregator<T> {

  @Override
  public Tuple toPartialTuple(T aggregate) {
    return toTuple(aggregate);
  }

  @Override
  public Tuple toFinalTuple(T aggregate) {
    return toTuple(aggregate);
  }

  public abstract Tuple toTuple(T aggregate);
}
