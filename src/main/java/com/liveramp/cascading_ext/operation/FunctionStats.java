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
import cascading.tuple.TupleEntry;
import com.liveramp.cascading_ext.operation.forwarding.ForwardingFunction;
import com.liveramp.cascading_ext.util.OperationStatsUtils;

public class FunctionStats extends ForwardingFunction {

  public static final String INPUT_RECORDS_COUNTER_NAME = "Input records";
  public static final String OUTPUT_RECORDS_COUNTER_NAME = "Output records";

  private final ForwardingFunctionCall wrapper = new ForwardingFunctionCall();
  private final String prefixInputRecords;
  private final String prefixOutputRecords;

  public FunctionStats(Function function) {
    this(OperationStatsUtils.getCounterNamePrefix(function), function);
  }

  public FunctionStats(Function function, String name) {
    this(OperationStatsUtils.getCounterNamePrefix(function, name), function);
  }

  @SuppressWarnings("unchecked")
  protected FunctionStats(String prefix, Function function) {
    super(function);
    this.prefixInputRecords = prefix + INPUT_RECORDS_COUNTER_NAME;
    this.prefixOutputRecords = prefix + OUTPUT_RECORDS_COUNTER_NAME;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void operate(FlowProcess process, FunctionCall call) {
    wrapper.setDelegate(call);
    super.operate(process, wrapper);
    process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefixInputRecords, 1);
    int output = wrapper.getOutputCollector().getCount();
    if (output > 0) {
      process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefixOutputRecords, output);
    }
  }

  public static class ForwardingFunctionCall<Context> extends OperationStatsUtils.ForwardingOperationCall<Context, FunctionCall<Context>>
      implements FunctionCall<Context> {

    @Override
    public TupleEntry getArguments() {
      return delegate.getArguments();
    }

    @Override
    public void setDelegate(FunctionCall<Context> delegate) {
      super.setDelegate(delegate);
      collector.setOutputCollector(delegate.getOutputCollector());
    }
  }
}
