package com.liveramp.cascading_ext.combiner;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.io.Serializable;

public interface PartialAggregator<T> extends Serializable {

  public T initialize();

  public T partialAggregate(T aggregate, TupleEntry nextValue);

  public Tuple toPartialTuple(T aggregate);
}
