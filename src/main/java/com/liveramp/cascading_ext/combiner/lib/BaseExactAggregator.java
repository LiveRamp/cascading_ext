package com.liveramp.cascading_ext.combiner.lib;

import cascading.tuple.Tuple;
import com.liveramp.cascading_ext.combiner.ExactAggregator;

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
