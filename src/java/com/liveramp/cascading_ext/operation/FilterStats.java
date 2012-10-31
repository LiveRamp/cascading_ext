package com.liveramp.cascading_ext.operation;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import com.liveramp.cascading_ext.operation.forwarding.ForwardingFilter;

// A FilterStats instance decorates a Filter instance and automatically
// maintains input/accepted/rejected records counters in addition to providing
// the functionality of the wrapped object.
public class FilterStats<Context> extends ForwardingFilter<Context> {
  public static final String INPUT_RECORDS_COUNTER_NAME = "Input records";
  public static final String ACCEPTED_RECORDS_COUNTER_NAME = "Accepted records";
  public static final String REJECTED_RECORDS_COUNTER_NAME = "Rejected records";

  private final String prefix;

  public FilterStats(Filter<Context> filter) {
    this(filter.getClass().getSimpleName() + " - ", filter);
  }

  public FilterStats(Filter<Context> filter, String name) {
    this(filter.getClass().getSimpleName() + " - " + name + " - ", filter);
  }

  protected FilterStats(String prefix, Filter<Context> filter){
    super(filter);
    this.prefix = prefix;
  }

  @Override
  public boolean isRemove(FlowProcess process, FilterCall<Context> call) {
    boolean isRemove = super.isRemove(process, call);
    process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefix+INPUT_RECORDS_COUNTER_NAME, 1);
    if (isRemove) {
      process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefix+REJECTED_RECORDS_COUNTER_NAME, 1);
    } else {
      process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefix+ACCEPTED_RECORDS_COUNTER_NAME, 1);
    }
    return isRemove;
  }
}
