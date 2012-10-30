package com.liveramp.cascading_ext.operation;

import cascading.flow.FlowProcess;
import cascading.operation.ConcreteCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.stats.FlowStats;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import com.liveramp.cascading_ext.operation.CascadingOperationStatsUtils.TupleEntryCollectorCounter;
import com.liveramp.cascading_ext.operation.forwarding.ForwardingFunction;

import java.io.IOException;
import java.io.Serializable;

// A FunctionStats instance decorates a Function instance and automatically
// maintains input/output records counters in addition to providing the
// functionality of the wrapped object.
public class FunctionStats<Context> extends ForwardingFunction<Context> {
  private final ForwardingFunctionCall<Context> wrapper = new ForwardingFunctionCall<Context>();

  public static final String INPUT_RECORDS_COUNTER_NAME = "Input records";

  public static final String OUTPUT_RECORDS_COUNTER_NAME = "Output records";
  private final String nameInputRecords;

  private final String nameOutputRecords;

  public FunctionStats(Function<Context> function) {
    super(function);
    String className = function.getClass().getSimpleName();
    this.nameInputRecords = className + " - " + INPUT_RECORDS_COUNTER_NAME;
    this.nameOutputRecords = className + " - " + OUTPUT_RECORDS_COUNTER_NAME;
  }

  public FunctionStats(Function<Context> function, String name) {
    super(function);
    String className = function.getClass().getSimpleName();
    this.nameInputRecords = className + " - " + name + " - " + INPUT_RECORDS_COUNTER_NAME;
    this.nameOutputRecords = className + " - " + name + " - " + OUTPUT_RECORDS_COUNTER_NAME;
  }

  @Override
  public void operate(FlowProcess process, FunctionCall<Context> call) {
    wrapper.setDelegate(call);
    super.operate(process, wrapper);
    process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameInputRecords, 1);
    int output = wrapper.getOutputCollector().getCount();
    if (output > 0) {
      process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameOutputRecords, output);
    }
  }

  public static class ForwardingFunctionCall<Context> extends CascadingOperationStatsUtils.ForwardingOperationCall<Context, FunctionCall<Context>>
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
