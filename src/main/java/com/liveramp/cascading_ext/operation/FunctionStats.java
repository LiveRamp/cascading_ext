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
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import com.liveramp.cascading_ext.operation.forwarding.ForwardingFunction;
import com.liveramp.cascading_ext.util.OperationStatsUtils;

public class FunctionStats extends ForwardingFunction {

  public static final String INPUT_RECORDS_COUNTER_NAME = "Input records";
  public static final String OUTPUT_RECORDS_COUNTER_NAME = "Output records";

  private final ForwardingFunctionCall wrapper = new ForwardingFunctionCall();
  private final String counterGroup;
  private final String inputRecordsCounterName;
  private final String outputRecordsCounterName;

  public FunctionStats(Function function) {
    this(OperationStatsUtils.getStackPosition(1), function);
  }

  public FunctionStats(StackTraceElement stackPosition, Function function) {
    this(stackPosition.getFileName(), stackPosition.getLineNumber() + " - " + function.getClass().getSimpleName(), function);
  }

  public FunctionStats(String counterName, Function function) {
    this(OperationStatsUtils.DEFAULT_COUNTER_CATEGORY, counterName, function);
  }

  @SuppressWarnings("unchecked")
  public FunctionStats(String counterGroup, String counterName, Function function) {
    super(function);
    this.counterGroup = counterGroup;
    this.inputRecordsCounterName = counterName + " - " + INPUT_RECORDS_COUNTER_NAME;
    this.outputRecordsCounterName = counterName + " - " + OUTPUT_RECORDS_COUNTER_NAME;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void operate(FlowProcess process, FunctionCall call) {
    wrapper.setDelegate(call);
    super.operate(process, wrapper);
    process.increment(counterGroup, inputRecordsCounterName, 1);
    int output = wrapper.getOutputCollector().getCount();
    process.increment(counterGroup, outputRecordsCounterName, output);
  }

  public static class ForwardingFunctionCall<Context> extends OperationStatsUtils.ForwardingOperationCall<Context, FunctionCall<Context>>
      implements FunctionCall<Context> {

    @Override
    public TupleEntry getArguments() {
      return delegate.getArguments();
    }

    @Override
    public Fields getDeclaredFields() {
      return delegate.getDeclaredFields();
    }

    @Override
    public void setDelegate(FunctionCall<Context> delegate) {
      super.setDelegate(delegate);
      collector.setOutputCollector(delegate.getOutputCollector());
    }
  }
}
