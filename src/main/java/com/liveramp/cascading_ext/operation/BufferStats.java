/**
 *  Copyright 2012 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.liveramp.cascading_ext.operation;

import cascading.flow.FlowProcess;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import com.liveramp.cascading_ext.operation.forwarding.ForwardingBuffer;
import com.liveramp.cascading_ext.util.OperationStatsUtils;

import java.util.Iterator;

public class BufferStats extends ForwardingBuffer {

  private final ForwardingBufferCall wrapper = new ForwardingBufferCall();

  public static final String INPUT_GROUPS_COUNTER_NAME = "Input groups";
  public static final String OUTPUT_RECORDS_COUNTER_NAME = "Output records";

  private final String counterGroup;
  private final String inputGroupsCounterName;
  private final String outputRecordsCounterName;

  public BufferStats(Buffer buffer) {
    this(OperationStatsUtils.getStackPosition(1), buffer);
  }

  public BufferStats(StackTraceElement stackPosition, Buffer buffer) {
    this(stackPosition.getFileName(), stackPosition.getLineNumber() + " - " + buffer.getClass().getSimpleName(), buffer);
  }

  public BufferStats(String counterName, Buffer buffer) {
    this(OperationStatsUtils.DEFAULT_COUNTER_CATEGORY, counterName, buffer);
  }

  @SuppressWarnings("unchecked")
  public BufferStats(String counterGroup, String counterName, Buffer buffer) {
    super(buffer);
    this.counterGroup = counterGroup;
    this.inputGroupsCounterName = counterName + " - " + INPUT_GROUPS_COUNTER_NAME;
    this.outputRecordsCounterName = counterName + " - " + OUTPUT_RECORDS_COUNTER_NAME;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void operate(FlowProcess process, BufferCall call) {
    wrapper.setDelegate(call);
    super.operate(process, wrapper);
    process.increment(counterGroup, inputGroupsCounterName, 1);
    int output = wrapper.getOutputCollector().getCount();
    process.increment(counterGroup, outputRecordsCounterName, output);
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
    public Fields getDeclaredFields() {
      return delegate.getDeclaredFields();
    }

    @Override
    public void setRetainValues(boolean retainValues) {
      delegate.setRetainValues(retainValues);
    }

    @Override
    public boolean isRetainValues() {
      return delegate.isRetainValues();
    }

    @Override
    public void setDelegate(BufferCall<Context> delegate) {
      super.setDelegate(delegate);
      collector.setOutputCollector(delegate.getOutputCollector());
    }
  }
}
