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
import com.liveramp.cascading_ext.util.OperationStatsUtils;

public class FilterStats extends ForwardingFilter {

  // Note: counter names are such that they make sense when sorted alphabetically
  public static final String INPUT_RECORDS_COUNTER_NAME = "Input records";
  public static final String KEPT_RECORDS_COUNTER_NAME = "Kept records";
  public static final String REMOVED_RECORDS_COUNTER_NAME = "Removed records";

  private final String prefixInputRecords;
  private final String prefixKeptRecords;
  private final String prefixRemovedRecords;

  public FilterStats(Filter filter) {
    this(OperationStatsUtils.getStackPosition(1) + " - " + filter.getClass().getSimpleName(), filter);
  }

  @SuppressWarnings("unchecked")
  public FilterStats(String name, Filter filter) {
    super(filter);
    this.prefixInputRecords = name + " - " + INPUT_RECORDS_COUNTER_NAME;
    this.prefixKeptRecords = name + " - " + KEPT_RECORDS_COUNTER_NAME;
    this.prefixRemovedRecords = name + " - " + REMOVED_RECORDS_COUNTER_NAME;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean isRemove(FlowProcess process, FilterCall call) {
    boolean isRemove = super.isRemove(process, call);
    process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefixInputRecords, 1);
    if (isRemove) {
      process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefixRemovedRecords, 1);
    } else {
      process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefixKeptRecords, 1);
    }
    return isRemove;
  }
}
