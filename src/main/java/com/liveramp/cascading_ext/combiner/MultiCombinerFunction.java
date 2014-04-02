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

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class MultiCombinerFunction
    extends BaseOperation<MultiCombinerFunctionContext>
    implements Function<MultiCombinerFunctionContext> {

  private final List<CombinerFunctionContext> contexts;
  private final Fields outputFields;

  public MultiCombinerFunction(List<CombinerDefinition> combinerDefinitions) {
    super(
        MultiCombiner.getInputFields(combinerDefinitions).size(),
        MultiCombiner.getIntermediateFields(combinerDefinitions));
    this.outputFields = MultiCombiner.getIntermediateFields(combinerDefinitions);
    contexts = Lists.newArrayList();
    for (CombinerDefinition combinerDefinition : combinerDefinitions) {
      contexts.add(new CombinerFunctionContext(combinerDefinition));
    }
  }

  @Override
  public void prepare(FlowProcess flow, OperationCall<MultiCombinerFunctionContext> call) {
    MultiCombinerFunctionContext contextList = new MultiCombinerFunctionContext(contexts);
    for (CombinerFunctionContext context : contextList.contexts) {
      context.prepare(flow);
    }
    call.setContext(contextList);
  }

  @Override
  public void operate(FlowProcess flow, FunctionCall<MultiCombinerFunctionContext> call) {
    flow.increment(Combiner.COUNTER_GROUP_NAME, Combiner.INPUT_TUPLES_COUNTER_NAME, 1);
    MultiCombinerFunctionContext contextList = call.getContext();
    for (CombinerFunctionContext context : contextList.contexts) {
      context.setGroupFields(call);
      context.setInputFields(call);
      context.combineAndEvict(flow, new OutputHandler(outputFields, flow, call));
    }
  }

  @Override
  public void flush(FlowProcess flow, OperationCall<MultiCombinerFunctionContext> call) {
    MultiCombinerFunctionContext contextList = call.getContext();
    for (CombinerFunctionContext context : contextList.contexts) {
      Iterator<Tuple> tuples = context.cacheTuplesIterator();
      while (tuples.hasNext()) {
        new OutputHandler(outputFields, flow, (FunctionCall<MultiCombinerFunctionContext>)call).handleOutput(context, tuples.next(), false);
        // Note: actively remove from the cache to save memory during cleanup
        tuples.remove();
      }
    }
  }

  private static class OutputHandler implements CombinerFunctionContext.OutputHandler {
    private final FlowProcess flow;
    private final FunctionCall<MultiCombinerFunctionContext> call;
    private final Fields outputFields;

    private OutputHandler(Fields outputFields, FlowProcess flow, FunctionCall<MultiCombinerFunctionContext> call) {
      this.outputFields = outputFields;
      this.flow = flow;
      this.call = call;
    }

    @Override
    public void handleOutput(CombinerFunctionContext context, Tuple tuple, boolean evicted) {
      if (evicted) {
        flow.increment(context.getCounterGroupName(), Combiner.EVICTED_TUPLES_COUNTER_NAME, 1);
      }
      flow.increment(context.getCounterGroupName(), Combiner.OUTPUT_TUPLES_COUNTER_NAME, 1);
      TupleEntry output = new TupleEntry(outputFields);
      output.setTuple(Tuple.size(outputFields.size()));

      MultiCombiner.populateOutputTupleEntry(context.getDefinition(), output, tuple);

      call.getOutputCollector().add(output);
    }
  }
}
