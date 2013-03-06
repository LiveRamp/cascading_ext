package com.liveramp.cascading_ext.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class AppendOperation extends BaseOperation implements Function {

  private final Object[] toPad;

  public AppendOperation(Fields toPad){
    super(toPad);
    this.toPad = new Object[toPad.size()];
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    functionCall.getOutputCollector().add(new Tuple(toPad));
  }
}
