package com.liveramp.cascading_ext.operator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;

public class Count extends Each {

  public Count(Pipe pipe, String counterName) {
    super(pipe, new CountFilterString(counterName));
  }

  public Count(Pipe pipe, String counterGroup, String counterName) {
    super(pipe, new CountFilterString(counterGroup, counterName));
  }

  public Count(Pipe pipe, Enum counter) {
    super(pipe, new CountFilterEnum(counter));
  }

  private static abstract class CountFilter extends BaseOperation implements Filter {

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
      increment(flowProcess);
      return false;
    }

    protected abstract void increment(FlowProcess flowProcess);
  }

  private static class CountFilterEnum extends CountFilter {

    private final Enum counter;

    public CountFilterEnum(Enum counter) {
      this.counter = counter;
    }

    @Override
    protected void increment(FlowProcess flowProcess) {
      flowProcess.increment(counter, 1);
    }
  }

  private static class CountFilterString extends CountFilter {

    private final String counterGroup;
    private final String counterName;

    public CountFilterString(String counterName) {
      this.counterGroup = "";
      this.counterName = counterName;
    }

    public CountFilterString(String counterGroup, String counterName) {
      this.counterGroup = counterGroup;
      this.counterName = counterName;
    }

    @Override
    protected void increment(FlowProcess flowProcess) {
      flowProcess.increment(counterGroup, counterName, 1);
    }
  }
}
