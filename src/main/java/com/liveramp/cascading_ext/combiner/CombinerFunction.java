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

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.liveramp.commons.util.MemoryUsageEstimator;

public class CombinerFunction<T> extends BaseOperation<CombinerFunctionContext<T>> implements Function<CombinerFunctionContext<T>> {

  private final CombinerDefinition<T> definition;

  public CombinerFunction(
      PartialAggregator<T> aggregator,
      Fields groupFields,
      Fields inputFields,
      Fields outputFields) {
    this(aggregator, groupFields, inputFields, outputFields, Combiner.DEFAULT_LIMIT, 0, null, null, Combiner.DEFAULT_STRICTNESS);
  }

  public CombinerFunction(
      PartialAggregator<T> aggregator,
      Fields groupFields,
      Fields inputFields,
      Fields outputFields,
      int limit) {
    this(aggregator, groupFields, inputFields, outputFields, limit, 0, null, null, Combiner.DEFAULT_STRICTNESS);
  }

  public CombinerFunction(
      PartialAggregator<T> aggregator,
      Fields groupFields,
      Fields inputFields,
      Fields outputFields,
      boolean strict) {
    this(aggregator, groupFields, inputFields, outputFields, Combiner.DEFAULT_LIMIT, 0, null, null, strict);
  }

  public CombinerFunction(
      PartialAggregator<T> aggregator,
      Fields groupFields,
      Fields inputFields,
      Fields outputFields,
      int itemLimit,
      long memoryLimit,
      MemoryUsageEstimator<Tuple> keySizeEstimator,
      MemoryUsageEstimator<T> valueSizeEstimator,
      boolean strict) {
    this(
        new CombinerDefinitionBuilder<T>()
            .setPartialAggregator(aggregator)
            .setGroupFields(groupFields)
            .setInputFields(inputFields)
            .setIntermediateFields(outputFields)
            .setLimit(itemLimit)
            .setMemoryLimit(memoryLimit)
            .setKeySizeEstimator(keySizeEstimator)
            .setValueSizeEstimator(valueSizeEstimator)
            .setStrict(strict)
            .get()
    );
  }

  public CombinerFunction(
      PartialAggregator<T> aggregator,
      Fields groupFields,
      Fields inputFields,
      Fields outputFields,
      int limit,
      boolean strict,
      Evictor<T> evictor) {
    this(
        new CombinerDefinitionBuilder<T>()
            .setPartialAggregator(aggregator)
            .setGroupFields(groupFields)
            .setInputFields(inputFields)
            .setIntermediateFields(outputFields)
            .setLimit(limit)
            .setStrict(strict)
            .setEvictor(evictor)
            .get()
    );
  }

  public CombinerFunction(CombinerDefinition<T> definition) {
    this(definition, definition.getInputFields().size(), definition.getGroupFields().append(definition.getIntermediateFields()));
  }

  public CombinerFunction(CombinerDefinition<T> definition, int numArgs, Fields outputFields) {
    super(numArgs, outputFields);
    this.definition = definition;
  }

  @Override
  public void prepare(FlowProcess flow, OperationCall<CombinerFunctionContext<T>> call) {
    CombinerFunctionContext<T> context = new CombinerFunctionContext<T>(definition);
    context.prepare(flow);
    call.setContext(context);
  }

  @Override
  public void operate(FlowProcess flow, FunctionCall<CombinerFunctionContext<T>> call) {
    CombinerFunctionContext<T> context = call.getContext();
    flow.increment(context.getCounterGroupName(), Combiner.INPUT_TUPLES_COUNTER_NAME, 1);
    context.setGroupFields(call);
    context.setInputFields(call);
    context.combineAndEvict(flow, new OutputHandler(flow, call));
  }

  @Override
  public void flush(FlowProcess flow, OperationCall<CombinerFunctionContext<T>> call) {

    CombinerFunctionContext<T> context = call.getContext();
    Iterator<Tuple> iterator = context.cacheTuplesIterator();
    while (iterator.hasNext()) {

      new OutputHandler(flow, (FunctionCall<CombinerFunctionContext<T>>)call).handleOutput(context, iterator.next(), false);
      // Note: actively remove from the cache to save memory during cleanup
      iterator.remove();
    }
  }

  private class OutputHandler implements CombinerFunctionContext.OutputHandler {
    private final FlowProcess flow;
    private final FunctionCall<CombinerFunctionContext<T>> call;

    private OutputHandler(FlowProcess flow, FunctionCall<CombinerFunctionContext<T>> call) {
      this.flow = flow;
      this.call = call;
    }

    @Override
    public void handleOutput(CombinerFunctionContext context, Tuple tuple, boolean evicted) {
      if (evicted) {
        flow.increment(context.getCounterGroupName(), Combiner.EVICTED_TUPLES_COUNTER_NAME, 1);
      }
      flow.increment(context.getCounterGroupName(), Combiner.OUTPUT_TUPLES_COUNTER_NAME, 1);
      call.getOutputCollector().add(tuple);
    }
  }

}
