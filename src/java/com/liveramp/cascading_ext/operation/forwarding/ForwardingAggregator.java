package com.liveramp.cascading_ext.operation.forwarding;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;

// A ForwardingAggregator wraps an Aggregator instance and implements the same
// interface by forwarding calls to the underlying object.
// An Aggregator Decorator can be easily implemented by subclassing the
// Forwarding class and overriding only the desired methods.
public class ForwardingAggregator<Context> extends ForwardingOperation<Context> implements Aggregator<Context> {

  private final Aggregator<Context> aggregator;

  public ForwardingAggregator(Aggregator<Context> aggregator) {
    super(aggregator);
    this.aggregator = aggregator;
  }

  @Override
  public void start(FlowProcess process, AggregatorCall<Context> call) {
    aggregator.start(process, call);
  }

  @Override
  public void aggregate(FlowProcess process, AggregatorCall<Context> call) {
    aggregator.aggregate(process, call);
  }

  @Override
  public void complete(FlowProcess process, AggregatorCall<Context> call) {
    aggregator.complete(process, call);
  }
}
