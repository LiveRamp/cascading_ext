package com.liveramp.cascading_ext.operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public abstract class BatchFunction<IN, OUT> extends BaseOperation<BatchFunction.Context<IN>> implements Function<BatchFunction.Context<IN>> {

  private int maxSizeOfBatch;

  public BatchFunction(Fields fieldDeclaration, int maxSizeOfBatch) {
    super(1, fieldDeclaration);
    this.maxSizeOfBatch = maxSizeOfBatch;
  }

  public static class Context<I> {
    List<I> input;

    public Context() {
      input = new ArrayList<I>();
    }
  }

  public abstract List<OUT> apply(FlowProcess flowProcess, List<IN> input);

  private void emitAll(FunctionCall<Context<IN>> functionCall, Collection<OUT> output) {
    for (OUT out : output) {
      functionCall.getOutputCollector().add(new Tuple(out));
    }
  }

  private void applyAndRefresh(FlowProcess flowProcess, FunctionCall<Context<IN>> functionCall) {
    Context<IN> context = functionCall.getContext();
    List<OUT> output = apply(flowProcess, context.input);
    emitAll(functionCall, output);
    context = new Context<IN>();
    functionCall.setContext(context);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Context<IN>> operationCall) {
    Context<IN> context = new Context<IN>();
    operationCall.setContext(context);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Context<IN>> functionCall) {
    TupleEntry arguments = functionCall.getArguments();
    IN arg = (IN) arguments.getObject(0);
    functionCall.getContext().input.add(arg);

    if (functionCall.getContext().input.size() >= maxSizeOfBatch) {
      applyAndRefresh(flowProcess, functionCall);
    }
  }


  @Override
  public void flush(FlowProcess flowProcess, OperationCall<Context<IN>> operationCall) {
    applyAndRefresh(flowProcess, (FunctionCall<Context<IN>>) operationCall);
  }

}
