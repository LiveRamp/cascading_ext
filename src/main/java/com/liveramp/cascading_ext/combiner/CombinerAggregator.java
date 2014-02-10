/**
 *  Copyright 2012 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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
