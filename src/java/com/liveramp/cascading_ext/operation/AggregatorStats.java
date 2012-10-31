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

  private final String prefix;

  public AggregatorStats(Aggregator<Context> aggregator) {
    this(aggregator.getClass().getSimpleName() + " - ", aggregator);
  }

  public AggregatorStats(Aggregator<Context> aggregator, String name) {
    this(aggregator.getClass().getSimpleName() +" - "+ name +" - ", aggregator);
  }

  protected AggregatorStats(String prefix, Aggregator<Context> aggregator){
    super(aggregator);
    this.prefix = prefix;
  }

  @Override
  public void start(FlowProcess process, AggregatorCall<Context> call) {
    super.start(process, call);
  }

  @Override
  public void aggregate(FlowProcess process, AggregatorCall<Context> call) {
    super.aggregate(process, wrapper);
    process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefix + INPUT_RECORDS_COUNTER_NAME, 1);
  }

  @Override
  public void complete(FlowProcess process, AggregatorCall<Context> call) {
    wrapper.setDelegate(call);
    super.complete(process, wrapper);
    int output = wrapper.getOutputCollector().getCount();
    if (output > 0) {
      process.increment(OperationStatsUtils.COUNTER_CATEGORY, prefix + TOTAL_OUTPUT_RECORDS_COUNTER_NAME, output);
    }
  }

  private static class ForwardingAggregatorCall<Context> extends OperationStatsUtils.ForwardingOperationCall<Context, AggregatorCall<Context>> implements AggregatorCall<Context> {

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
