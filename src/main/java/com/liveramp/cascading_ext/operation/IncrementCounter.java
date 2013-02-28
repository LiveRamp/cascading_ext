package com.liveramp.cascading_ext.operation;


import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;

public class IncrementCounter extends BaseOperation implements Filter {

  private final String fieldName;
  private final String counterGroup;
  private final String counterName;

  public IncrementCounter(String fieldName, String counterGroup, String counterName) {
    this.fieldName = fieldName;
    this.counterGroup = counterGroup;
    this.counterName = counterName;
  }

  @Override
  public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
    Long value = filterCall.getArguments().getLong(fieldName);
    flowProcess.increment(counterGroup, counterName, value);
    return false;
  }
}
