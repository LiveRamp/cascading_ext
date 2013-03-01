package com.liveramp.cascading_ext.operator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;

public class Increment extends Each {

  public Increment(Pipe pipe, String counterName) {
    super(pipe, new IncrementFilterString(counterName));
  }

  public Increment(Pipe pipe, String counterGroup, String counterName) {
    super(pipe, new IncrementFilterString(counterGroup, counterName));
  }

  public Increment(Pipe pipe, Enum counter) {
    super(pipe, new IncrementFilterEnum(counter));
  }

  private static abstract class IncrementFilter extends BaseOperation implements Filter {

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
      increment(flowProcess);
      return false;
    }

    protected abstract void increment(FlowProcess flowProcess);
  }

  private static class IncrementFilterEnum extends IncrementFilter {

    private final Enum counter;

    public IncrementFilterEnum(Enum counter) {
      this.counter = counter;
    }

    @Override
    protected void increment(FlowProcess flowProcess) {
      flowProcess.increment(counter, 1);
    }
  }

  private static class IncrementFilterString extends IncrementFilter {

    private final String counterGroup;
    private final String counterName;

    public IncrementFilterString(String counterName) {
      this.counterGroup = "";
      this.counterName = counterName;
    }

    public IncrementFilterString(String counterGroup, String counterName) {
      this.counterGroup = counterGroup;
      this.counterName = counterName;
    }

    @Override
    protected void increment(FlowProcess flowProcess) {
      flowProcess.increment(counterGroup, counterName, 1);
    }
  }
}
