package com.liveramp.cascading_ext.operation;

import cascading.flow.FlowProcess;
import cascading.operation.*;
import cascading.tuple.TupleEntry;
import com.liveramp.cascading_ext.operation.forwarding.ForwardingBuffer;

import java.util.Iterator;

// A BufferStats instance decorates a Buffer instance and automatically
// maintains input/output records counters in addition to providing the
// functionality of the wrapped object.
public class BufferStats<Context> extends ForwardingBuffer<Context> {
  private final ForwardingBufferCall<Context> wrapper = new ForwardingBufferCall<Context>();

  public static final String INPUT_RECORDS_COUNTER_NAME = "Input groups";
  public static final String OUTPUT_RECORDS_COUNTER_NAME = "Output records";

  private final String prefix;

  public BufferStats(Buffer<Context> buffer) {
    this(buffer.getClass().getSimpleName()+" - ", buffer);
  }

  public BufferStats(Buffer<Context> buffer, String name) {
    this(buffer.getClass().getSimpleName() + " - " + name +" - ", buffer);
  }

  protected BufferStats(String prefix, Buffer<Context> buffer) {
    super(buffer);
    this.prefix = prefix;
  }

  @Override
  public void operate(FlowProcess process, BufferCall<Context> call) {
    wrapper.setDelegate(call);
    super.operate(process, wrapper);
    process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefix + INPUT_RECORDS_COUNTER_NAME, 1);
    int output = wrapper.getOutputCollector().getCount();
    if (output > 0) {
      process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefix + OUTPUT_RECORDS_COUNTER_NAME, output);
    }
  }

  private static class ForwardingBufferCall<Context> extends OperationStatsUtils.ForwardingOperationCall<Context, BufferCall<Context>> implements BufferCall<Context> {

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
