package com.liveramp.cascading_ext.combiner;

import cascading.operation.AggregatorCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.ArrayList;

public class CombinerAggregatorContext<T> implements Serializable {

  private final CombinerDefinition<T> definition;

  private T aggregate;
  private TupleEntry key;
  private TupleEntry input;
  private ArrayList<Integer> keyFieldsPos;
  private ArrayList<Integer> inputFieldsPos;

  public CombinerAggregatorContext(CombinerDefinition<T> definition) {
    this.definition = definition;
  }

  public void start() {
    aggregate = definition.getFinalAggregator().initialize();
    key = new TupleEntry(definition.getGroupFields(), Tuple.size(definition.getGroupFields().size()));
    input = new TupleEntry(definition.getIntermediateFields(), Tuple.size(definition.getIntermediateFields().size()));
    keyFieldsPos = Lists.newArrayList();
    inputFieldsPos = Lists.newArrayList();
  }

  public void setGroupFields(AggregatorCall call) {
    CombinerUtils.setTupleEntry(key, keyFieldsPos, definition.getGroupFields(), call.getGroup());
  }

  public void setInputFields(AggregatorCall call) {
    CombinerUtils.setTupleEntry(input, inputFieldsPos, definition.getIntermediateFields(), call.getArguments());
  }

  public void aggregate() {
    aggregate = definition.getFinalAggregator().finalAggregate(aggregate, input);
  }

  public Tuple getAggregateTuple() {
    return key.getTuple().append(definition.getFinalAggregator().toFinalTuple(aggregate));
  }

  public CombinerDefinition<T> getDefinition() {
    return definition;
  }
}
