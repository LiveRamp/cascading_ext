package com.liveramp.cascading_ext.operation;

import cascading.flow.FlowProcess;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.ConcreteCall;
import cascading.stats.FlowStats;
import com.liveramp.cascading_ext.operation.forwarding.ForwardingBuffer;

// A BufferStats instance decorates a Buffer instance and automatically
// maintains input/output records counters in addition to providing the
// functionality of the wrapped object.
public class BufferStats extends ForwardingBuffer {

  public static final String INPUT_RECORDS_COUNTER_NAME = "Input records";
  public static final String OUTPUT_RECORDS_COUNTER_NAME = "Output records";

  private final String nameInputRecords;
  private final String nameOutputRecords;

  public BufferStats(Buffer buffer) {
    super(buffer);
    String className = buffer.getClass().getSimpleName();
    this.nameInputRecords = className + " - " + INPUT_RECORDS_COUNTER_NAME;
    this.nameOutputRecords = className + " - " + OUTPUT_RECORDS_COUNTER_NAME;
  }

  public BufferStats(Buffer buffer, String name) {
    super(buffer);
    String className = buffer.getClass().getSimpleName();
    this.nameInputRecords = className + " - " + name + INPUT_RECORDS_COUNTER_NAME;
    this.nameOutputRecords = className + " - " + name + OUTPUT_RECORDS_COUNTER_NAME;
  }

  @Override
  public void operate(FlowProcess process, BufferCall call) {
    CascadingOperationStatsUtils.TupleEntryCollectorCounter outputCollectorCounter = new CascadingOperationStatsUtils.TupleEntryCollectorCounter(call.getOutputCollector());
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
