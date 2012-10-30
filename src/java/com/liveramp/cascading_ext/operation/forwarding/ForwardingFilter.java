package com.liveramp.cascading_ext.operation.forwarding;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;

// A ForwardingFilter wraps a Filter instance and implements the same
// interface by forwarding calls to the underlying object.
// A Filter Decorator can be easily implemented by subclassing the
// Forwarding class and overriding only the desired methods.
public class ForwardingFilter <C> extends ForwardingOperation<C> implements Filter<C> {

  private final Filter<C> filter;

  public ForwardingFilter(Filter<C> filter) {
    super(filter);
    this.filter = filter;
  }

  @Override
  public boolean isRemove(FlowProcess process, FilterCall<C> call) {
    return filter.isRemove(process, call);
  }
}
