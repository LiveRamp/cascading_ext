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

public abstract class BatchFunction<S, T> extends BaseOperation<BatchFunction.Context<S>> implements Function<BatchFunction.Context<S>> {

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

  public abstract List<T> apply(FlowProcess flowProcess, List<S> input);

  private void emitAll(FunctionCall<Context<S>> functionCall, Collection<T> output) {
    for (T t : output) {
      functionCall.getOutputCollector().add(new Tuple(t));
    }
  }

  private void applyAndRefresh(FlowProcess flowProcess, FunctionCall<Context<S>> functionCall) {
    Context<S> context = functionCall.getContext();
    List<T> output = apply(flowProcess, context.input);
    emitAll(functionCall, output);
    context = new Context<S>();
    functionCall.setContext(context);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Context<S>> operationCall) {
    Context<S> context = new Context<S>();
    operationCall.setContext(context);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void operate(FlowProcess flowProcess, FunctionCall<Context<S>> functionCall) {
    TupleEntry arguments = functionCall.getArguments();
    S arg = (S) arguments.getObject(0);
    functionCall.getContext().input.add(arg);

    if (functionCall.getContext().input.size() >= maxSizeOfBatch) {
      applyAndRefresh(flowProcess, functionCall);
    }
  }


  @Override
  public void flush(FlowProcess flowProcess, OperationCall<Context<S>> operationCall) {
    applyAndRefresh(flowProcess, (FunctionCall<Context<S>>) operationCall);
  }

}
