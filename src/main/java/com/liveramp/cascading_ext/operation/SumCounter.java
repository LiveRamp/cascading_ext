package com.liveramp.cascading_ext.operation;


import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;

public class SumCounter extends BaseOperation implements Filter {

  private final String fieldName;
  private final String counterGroup;
  private final String counterName;
  private final Enum counterEnum;

  public SumCounter(String fieldName, String counterName) {
    this.fieldName = fieldName;
    this.counterGroup = "";
    this.counterName = counterName;
    this.counterEnum = null;
  }

  public SumCounter(String fieldName, String counterGroup, String counterName) {
    this.fieldName = fieldName;
    this.counterGroup = counterGroup;
    this.counterName = counterName;
    this.counterEnum = null;
  }

  public SumCounter(String fieldName, Enum counter) {
    this.fieldName = fieldName;
    this.counterGroup = null;
    this.counterName = null;
    this.counterEnum = counter;
  }

  @Override
  public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
    Long value = filterCall.getArguments().getLong(fieldName);
    if (counterEnum == null) {
      flowProcess.increment(counterGroup, counterName, value);
    } else {
      flowProcess.increment(counterEnum, value);
    }
    return false;
  }
}
