package com.liveramp.cascading_ext;

import cascading.flow.FlowStepStrategy;

public interface FlowStepStrategyFactory<T> {
  public FlowStepStrategy<T> getFlowStepStrategy();
}
