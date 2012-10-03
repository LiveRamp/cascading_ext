package com.liveramp.cascading_ext.operation;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.stats.FlowStats;
import com.liveramp.cascading_ext.operation.forwarding.ForwardingFilter;

// A FilterStats instance decorates a Filter instance and automatically
// maintains input/accepted/rejected records counters in addition to providing
// the functionality of the wrapped object.
public class FilterStats extends ForwardingFilter {

  public static final String INPUT_RECORDS_COUNTER_NAME = "Input records";
  public static final String ACCEPTED_RECORDS_COUNTER_NAME = "Accepted records";
  public static final String REJECTED_RECORDS_COUNTER_NAME = "Rejected records";

  private final String nameInputRecords;
  private final String nameAcceptedRecords;
  private final String nameRejectedRecords;

  public FilterStats(Filter filter) {
    super(filter);
    String className = filter.getClass().getSimpleName();
    this.nameInputRecords = className + " - " + INPUT_RECORDS_COUNTER_NAME;
    this.nameAcceptedRecords = className + " - " + ACCEPTED_RECORDS_COUNTER_NAME;
    this.nameRejectedRecords = className + " - " + REJECTED_RECORDS_COUNTER_NAME;
  }

  public FilterStats(Filter filter, String name) {
    super(filter);
    String className = filter.getClass().getSimpleName();
    this.nameInputRecords = className + " - " + name + INPUT_RECORDS_COUNTER_NAME;
    this.nameAcceptedRecords = className + " - " + name + ACCEPTED_RECORDS_COUNTER_NAME;
    this.nameRejectedRecords = className + " - " + name + REJECTED_RECORDS_COUNTER_NAME;
  }

  @Override
  public boolean isRemove(FlowProcess process, FilterCall call) {
    boolean isRemove = super.isRemove(process, call);
    process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, this.nameInputRecords, 1);
    if (isRemove) {
      process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, this.nameRejectedRecords, 1);
    } else {
      process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, this.nameAcceptedRecords, 1);
    }
    return isRemove;
  }

  public long getInputRecords(FlowStats flowStats) {
    return flowStats.getCounterValue(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameInputRecords);
  }

  public long getAcceptedRecords(FlowStats flowStats) {
    return flowStats.getCounterValue(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameAcceptedRecords);
  }

  public long getRejectedRecords(FlowStats flowStats) {
    return flowStats.getCounterValue(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameRejectedRecords);
  }
}
