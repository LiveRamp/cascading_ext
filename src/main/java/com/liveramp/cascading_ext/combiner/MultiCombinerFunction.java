package com.liveramp.cascading_ext.combiner;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

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
      context.prepare();
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
      List<Tuple> tuples = context.combineAndEvict(flow);
      if (tuples != null) {
        flow.increment(context.getCounterGroupName(), Combiner.EVICTED_TUPLES_COUNTER_NAME, tuples.size());
        for (Tuple tuple : tuples) {
          emitTuple(context, tuple, flow, call);
        }
      }
    }
  }

  @Override
  public void flush(FlowProcess flow, OperationCall<MultiCombinerFunctionContext> call) {
    MultiCombinerFunctionContext contextList = call.getContext();
    for (CombinerFunctionContext context : contextList.contexts) {
      Iterator<Tuple> tuples = context.cacheTuplesIterator();
      while (tuples.hasNext()) {
        emitTuple(context, tuples.next(), flow, (FunctionCall) call);
        // Note: actively remove from the cache to save memory during cleanup
        tuples.remove();
      }
    }
  }

  protected void emitTuple(CombinerFunctionContext context, Tuple tuple, FlowProcess flow, FunctionCall call) {
    flow.increment(context.getCounterGroupName(), Combiner.OUTPUT_TUPLES_COUNTER_NAME, 1);
    TupleEntry output = new TupleEntry(outputFields);
    output.setTuple(Tuple.size(outputFields.size()));

    MultiCombiner.populateOutputTupleEntry(context.getDefinition(), output, tuple);

    call.getOutputCollector().add(output);
  }
}
