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
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.collect.Lists;

import java.util.List;

public class MultiCombinerAggregator extends BaseOperation<CombinerAggregatorContext> implements Aggregator<CombinerAggregatorContext> {

  private Fields outputFields;
  protected final List<CombinerAggregatorContext> contexts;

  public MultiCombinerAggregator(List<CombinerDefinition> combinerDefinitions) {
    super(MultiCombiner.getIntermediateFields(combinerDefinitions).size(), MultiCombiner.getOutputFields(combinerDefinitions));
    this.outputFields = MultiCombiner.getOutputFields(combinerDefinitions);
    contexts = Lists.newArrayList();
    for (CombinerDefinition combinerDefinition : combinerDefinitions) {
      contexts.add(new CombinerAggregatorContext(combinerDefinition));
    }
  }

  @Override
  public void start(FlowProcess flowProcess, AggregatorCall<CombinerAggregatorContext> aggregatorCall) {
    for (CombinerAggregatorContext context : contexts) {
      if (shouldUseThisCombiner(context, aggregatorCall)) {
        context.start();
        context.setGroupFields(aggregatorCall);
        aggregatorCall.setContext(context);
        break;
      }
    }
  }

  @Override
  public void aggregate(FlowProcess flowProcess, AggregatorCall<CombinerAggregatorContext> aggregatorCall) {
    CombinerAggregatorContext context = aggregatorCall.getContext();
    context.setInputFields(aggregatorCall);
    context.aggregate();
  }

  @Override
  public void complete(FlowProcess flowProcess, AggregatorCall<CombinerAggregatorContext> aggregatorCall) {
    CombinerAggregatorContext context = aggregatorCall.getContext();
    emitTuple(context, aggregatorCall);
  }

  protected boolean shouldUseThisCombiner(CombinerAggregatorContext context, AggregatorCall<CombinerAggregatorContext> aggregatorCall) {
    return context.getDefinition().getId() == aggregatorCall.getGroup().getInteger(MultiCombiner.ID_FIELD);
  }

  protected void emitTuple(CombinerAggregatorContext context, AggregatorCall aggregatorCall) {
    TupleEntry output = new TupleEntry(outputFields);
    output.setTuple(Tuple.size(outputFields.size()));

    Tuple result = context.getAggregateTuple();
    MultiCombiner.populateOutputTupleEntry(context.getDefinition(), output, result);

    aggregatorCall.getOutputCollector().add(output);
  }
}
