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
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import com.liveramp.cascading_ext.operation.forwarding.ForwardingFilter;

public class FilterStats<Context> extends ForwardingFilter<Context> {

  // Note: counter names are such that they make sense when sorted alphabetically
  public static final String INPUT_RECORDS_COUNTER_NAME = "Input records";
  public static final String ACCEPTED_RECORDS_COUNTER_NAME = "Kept records";
  public static final String REJECTED_RECORDS_COUNTER_NAME = "Removed records";

  private final String prefix;

  public FilterStats(Filter<Context> filter) {
    this(OperationStatsUtils.getCounterNamePrefix(filter), filter);
  }

  public FilterStats(Filter<Context> filter, String name) {
    this(OperationStatsUtils.getCounterNamePrefix(filter, name), filter);
  }

  protected FilterStats(String prefix, Filter<Context> filter) {
    super(filter);
    this.prefix = prefix;
  }

  @Override
  public boolean isRemove(FlowProcess process, FilterCall<Context> call) {
    boolean isRemove = super.isRemove(process, call);
    process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefix + INPUT_RECORDS_COUNTER_NAME, 1);
    if (isRemove) {
      process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefix + REJECTED_RECORDS_COUNTER_NAME, 1);
    } else {
      process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefix + ACCEPTED_RECORDS_COUNTER_NAME, 1);
    }
    return isRemove;
  }
}
