package com.liveramp.cascading_ext.combiner;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;

public class CombinerAggregator<T> extends BaseOperation<CombinerAggregatorContext<T>> implements Aggregator<CombinerAggregatorContext<T>> {

  private final CombinerDefinition<T> combinerDefinition;

  protected CombinerAggregator(CombinerDefinition<T> combinerDefinition) {
    super(combinerDefinition.getIntermediateFields().size(),
        combinerDefinition.getGroupFields().append(combinerDefinition.getOutputFields()));
    this.combinerDefinition = combinerDefinition;
  }

  public CombinerAggregator(FinalAggregator<T> aggregator, Fields groupFields, Fields inputFields, Fields outputFields) {
    this(new CombinerDefinitionBuilder<T>()
        .setFinalAggregator(aggregator)
        .setGroupFields(groupFields)
        .setIntermediateFields(inputFields)
        .setOutputFields(outputFields)
        .get());
  }

  @Override
  public void start(FlowProcess flowProcess, AggregatorCall<CombinerAggregatorContext<T>> aggregatorCall) {
    CombinerAggregatorContext<T> context = new CombinerAggregatorContext<T>(combinerDefinition);
    context.start();
    context.setGroupFields(aggregatorCall);
    aggregatorCall.setContext(context);
  }

  @Override
  public void aggregate(FlowProcess flowProcess, AggregatorCall<CombinerAggregatorContext<T>> aggregatorCall) {
    CombinerAggregatorContext context = aggregatorCall.getContext();
    context.setInputFields(aggregatorCall);
    context.aggregate();
  }

  @Override
  public void complete(FlowProcess flowProcess, AggregatorCall<CombinerAggregatorContext<T>> aggregatorCall) {
    CombinerAggregatorContext<T> context = aggregatorCall.getContext();
    aggregatorCall.getOutputCollector().add(context.getAggregateTuple());
  }
}
