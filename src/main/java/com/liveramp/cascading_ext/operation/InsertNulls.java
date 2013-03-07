package com.liveramp.cascading_ext.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;


public class InsertNulls extends BaseOperation implements Function {

  private Fields fieldsToAdd;

  public InsertNulls(Fields fieldsToAdd) {
    super(fieldsToAdd);
    this.fieldsToAdd = fieldsToAdd;
  }


  @Override
  public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    Tuple output = new Tuple();
    for (int i = 0; i < fieldsToAdd.size(); i++) {
      output.add(null);
    }
    functionCall.getOutputCollector().add(output);
  }
}
