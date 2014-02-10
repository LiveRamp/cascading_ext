package com.liveramp.cascading_ext.combiner;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.io.Serializable;

public interface FinalAggregator<T> extends Serializable {

  public T initialize();

  public T finalAggregate(T aggregate, TupleEntry partialAggregate);

  public Tuple toFinalTuple(T aggregate);
}
