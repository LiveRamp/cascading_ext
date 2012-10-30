package com.liveramp.cascading_ext.operation;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.tuple.TupleEntry;
import com.liveramp.cascading_ext.operation.forwarding.ForwardingAggregator;

// An AggregatorStats instance decorates an Aggregator instance and
// automatically maintains input/output records counters in addition to
// providing the functionality of the wrapped object.
public class AggregatorStats <Context> extends ForwardingAggregator<Context> {
  private final ForwardingAggregatorCall<Context> wrapper = new ForwardingAggregatorCall<Context>();

  public static final String INPUT_RECORDS_COUNTER_NAME = "Input records";
  public static final String TOTAL_OUTPUT_RECORDS_COUNTER_NAME = "Total output records";

  private final String nameInputRecords;
  private final String nameTotalOutputRecords;

  public AggregatorStats(Aggregator<Context> aggregator) {
    super(aggregator);
    String className = aggregator.getClass().getSimpleName();
    this.nameInputRecords = className + " - " + INPUT_RECORDS_COUNTER_NAME;
    this.nameTotalOutputRecords = className + " - " + TOTAL_OUTPUT_RECORDS_COUNTER_NAME;
  }

  public AggregatorStats(Aggregator<Context> aggregator, String name) {
    super(aggregator);
    String className = aggregator.getClass().getSimpleName();
    this.nameInputRecords = className + " - " + name + " - " + INPUT_RECORDS_COUNTER_NAME;
    this.nameTotalOutputRecords = className + " - " + name + " - " + TOTAL_OUTPUT_RECORDS_COUNTER_NAME;
  }

  @Override
  public void start(FlowProcess process, AggregatorCall<Context> call) {
    wrapper.setDelegate(call);
    super.start(process, wrapper);

    int output = wrapper.getOutputCollector().getCount();
    if (output > 0) {
      process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameTotalOutputRecords, output);
    }
  }

  @Override
  public void aggregate(FlowProcess process, AggregatorCall<Context> call) {
    wrapper.setDelegate(call);
    super.aggregate(process, wrapper);
    int output = wrapper.getOutputCollector().getCount();
    process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameInputRecords, 1);
    if (output > 0) {
      process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameTotalOutputRecords, output);
    }
  }

  @Override
  public void complete(FlowProcess process, AggregatorCall<Context> call) {
    wrapper.setDelegate(call);
    super.complete(process, wrapper);
    int output = wrapper.getOutputCollector().getCount();
    if (output > 0) {
      process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameTotalOutputRecords, output);
    }
  }

  private static class ForwardingAggregatorCall<Context> extends CascadingOperationStatsUtils.ForwardingOperationCall<Context, AggregatorCall<Context>> implements AggregatorCall<Context> {

    @Override
    public TupleEntry getGroup() {
      return delegate.getGroup();
    }

    @Override
    public TupleEntry getArguments() {
      return delegate.getArguments();
    }

    @Override
    public void setDelegate(AggregatorCall<Context> delegate){
      super.setDelegate(delegate);
      collector.setOutputCollector(delegate.getOutputCollector());
    }
  }
}
