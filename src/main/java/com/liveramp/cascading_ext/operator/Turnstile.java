package com.liveramp.cascading_ext.operator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;

public class Turnstile extends Each {

  public Turnstile(Pipe pipe, String counterName) {
    super(pipe, new TurnstileFilterString(counterName));
  }

  public Turnstile(Pipe pipe, String counterGroup, String counterName) {
    super(pipe, new TurnstileFilterString(counterGroup, counterName));
  }

  public Turnstile(Pipe pipe, Enum counter) {
    super(pipe, new TurnstileFilterEnum(counter));
  }

  private static abstract class TurnstileFilter extends BaseOperation implements Filter {

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
      increment(flowProcess);
      return false;
    }

    protected abstract void increment(FlowProcess flowProcess);
  }

  private static class TurnstileFilterEnum extends TurnstileFilter {

    private final Enum counter;

    public TurnstileFilterEnum(Enum counter) {
      this.counter = counter;
    }

    @Override
    protected void increment(FlowProcess flowProcess) {
      flowProcess.increment(counter, 1);
    }
  }

  private static class TurnstileFilterString extends TurnstileFilter {

    private final String counterGroup;
    private final String counterName;

    public TurnstileFilterString(String counterName) {
      this.counterGroup = "";
      this.counterName = counterName;
    }

    public TurnstileFilterString(String counterGroup, String counterName) {
      this.counterGroup = counterGroup;
      this.counterName = counterName;
    }

    @Override
    protected void increment(FlowProcess flowProcess) {
      flowProcess.increment(counterGroup, counterName, 1);
    }
  }
}
