package com.liveramp.cascading_ext.operation;

import cascading.flow.FlowProcess;
import cascading.operation.ConcreteCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.stats.FlowStats;

import com.liveramp.cascading_ext.operation.CascadingOperationStatsUtils.TupleEntryCollectorCounter;
import com.liveramp.cascading_ext.operation.forwarding.ForwardingFunction;

// A FunctionStats instance decorates a Function instance and automatically
// maintains input/output records counters in addition to providing the
// functionality of the wrapped object.
public class FunctionStats extends ForwardingFunction {

  public static final String INPUT_RECORDS_COUNTER_NAME = "Input records";
  public static final String OUTPUT_RECORDS_COUNTER_NAME = "Output records";

  private final String nameInputRecords;
  private final String nameOutputRecords;

  public FunctionStats(Function function) {
    super(function);
    String className = function.getClass().getSimpleName();
    this.nameInputRecords = className + " - " + INPUT_RECORDS_COUNTER_NAME;
    this.nameOutputRecords = className + " - " + OUTPUT_RECORDS_COUNTER_NAME;
  }

  public FunctionStats(Function function, String name) {
    super(function);
    String className = function.getClass().getSimpleName();
    this.nameInputRecords = className + " - " + name + " - " + INPUT_RECORDS_COUNTER_NAME;
    this.nameOutputRecords = className + " - " + name + " - " + OUTPUT_RECORDS_COUNTER_NAME;
  }

  @Override
  public void operate(FlowProcess process, FunctionCall call) {
    TupleEntryCollectorCounter outputCollectorCounter = new TupleEntryCollectorCounter(call.getOutputCollector());
    super.operate(process, CascadingOperationStatsUtils.copyConcreteCallAndSetOutputCollector((ConcreteCall) call, outputCollectorCounter));
    process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameInputRecords, 1);
    if (outputCollectorCounter.getCount() > 0) {
      process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameOutputRecords, outputCollectorCounter.getCount());
    }
  }

  public long getInputRecords(FlowStats flowStats) {
    return flowStats.getCounterValue(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameInputRecords);
  }

  public long getOutputRecords(FlowStats flowStats) {
    return flowStats.getCounterValue(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameOutputRecords);
  }
}
