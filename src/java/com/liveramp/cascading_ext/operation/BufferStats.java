package com.liveramp.cascading_ext.operation;

import cascading.flow.FlowProcess;
import cascading.operation.*;
import cascading.stats.FlowStats;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import com.liveramp.cascading_ext.operation.forwarding.ForwardingBuffer;

import java.util.Iterator;

// A BufferStats instance decorates a Buffer instance and automatically
// maintains input/output records counters in addition to providing the
// functionality of the wrapped object.
public class BufferStats<Context> extends ForwardingBuffer<Context> {
  private final ForwardingBufferCall<Context> wrapper = new ForwardingBufferCall<Context>();

  public static final String INPUT_RECORDS_COUNTER_NAME = "Input groups";
  public static final String OUTPUT_RECORDS_COUNTER_NAME = "Output records";

  private final String nameInputRecords;
  private final String nameOutputRecords;

  public BufferStats(Buffer<Context> buffer) {
    super(buffer);
    String className = buffer.getClass().getSimpleName();
    this.nameInputRecords = className + " - " + INPUT_RECORDS_COUNTER_NAME;
    this.nameOutputRecords = className + " - " + OUTPUT_RECORDS_COUNTER_NAME;
  }

  public BufferStats(Buffer<Context> buffer, String name) {
    super(buffer);
    String className = buffer.getClass().getSimpleName();
    this.nameInputRecords = className + " - " + name + INPUT_RECORDS_COUNTER_NAME;
    this.nameOutputRecords = className + " - " + name + OUTPUT_RECORDS_COUNTER_NAME;
  }

  @Override
  public void operate(FlowProcess process, BufferCall<Context> call) {
    wrapper.setDelegate(call);
    super.operate(process, wrapper);
    process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameInputRecords, 1);
    int output = wrapper.getOutputCollector().getCount();
    if (output > 0) {
      process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameOutputRecords, output);
    }
  }

  private static class ForwardingBufferCall<Context> extends CascadingOperationStatsUtils.ForwardingOperationCall<Context, BufferCall<Context>> implements BufferCall<Context> {

    @Override
    public TupleEntry getGroup() {
      return delegate.getGroup();
    }

    @Override
    public Iterator<TupleEntry> getArgumentsIterator() {
      return delegate.getArgumentsIterator();
    }

    @Override
    public void setDelegate(BufferCall<Context> delegate){
      super.setDelegate(delegate);
      collector.setOutputCollector(delegate.getOutputCollector());
    }
  }
}
