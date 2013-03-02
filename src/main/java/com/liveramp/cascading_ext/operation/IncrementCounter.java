package com.liveramp.cascading_ext.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;

public class IncrementCounter extends BaseOperation implements Filter {

  private final String counterGroup;
  private final String counterName;
  private final Enum counterEnum;

  public IncrementCounter(String counterName) {
    this.counterGroup = "";
    this.counterName = counterName;
    this.counterEnum = null;
  }

  public IncrementCounter(String counterGroup, String counterName) {
    this.counterGroup = counterGroup;
    this.counterName = counterName;
    this.counterEnum = null;
  }

  public IncrementCounter(Enum counter) {
    this.counterGroup = null;
    this.counterName = null;
    this.counterEnum = counter;
  }

  @Override
  public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
    if (counterEnum == null) {
      flowProcess.increment(counterGroup, counterName, 1);
    } else {
      flowProcess.increment(counterEnum, 1);
    }
    return false;
  }
}
