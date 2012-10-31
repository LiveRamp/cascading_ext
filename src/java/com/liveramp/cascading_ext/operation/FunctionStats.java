package com.liveramp.cascading_ext.operation;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;

import cascading.tuple.TupleEntry;
import com.liveramp.cascading_ext.operation.forwarding.ForwardingFunction;

// A FunctionStats instance decorates a Function instance and automatically
// maintains input/output records counters in addition to providing the
// functionality of the wrapped object.
public class FunctionStats<Context> extends ForwardingFunction<Context> {
  public static final String INPUT_RECORDS_COUNTER_NAME = "Input records";
  public static final String OUTPUT_RECORDS_COUNTER_NAME = "Output records";

  private final ForwardingFunctionCall<Context> wrapper = new ForwardingFunctionCall<Context>();
  private final String prefix;

  public FunctionStats(Function<Context> function) {
    this(function.getClass().getSimpleName()+" - ", function);
  }

  public FunctionStats(Function<Context> function, String name) {
    this(function.getClass().getSimpleName() + " - " + name + " - ", function);
  }

  protected FunctionStats(String prefix, Function<Context> function){
    super(function);
    this.prefix = prefix;
  }

  @Override
  public void operate(FlowProcess process, FunctionCall<Context> call) {
    wrapper.setDelegate(call);
    super.operate(process, wrapper);
    process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefix + INPUT_RECORDS_COUNTER_NAME, 1);
    int output = wrapper.getOutputCollector().getCount();
    if (output > 0) {
      process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefix + OUTPUT_RECORDS_COUNTER_NAME, output);
    }
  }

  public static class ForwardingFunctionCall<Context> extends OperationStatsUtils.ForwardingOperationCall<Context, FunctionCall<Context>>
      implements FunctionCall<Context> {

    @Override
    public TupleEntry getArguments() {
      return delegate.getArguments();
    }

    @Override
    public void setDelegate(FunctionCall<Context> delegate){
      super.setDelegate(delegate);
      collector.setOutputCollector(delegate.getOutputCollector());
    }
  }
}
