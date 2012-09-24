package com.liveramp.cascading_ext.flow_step_strategy;

import cascading.flow.FlowStepStrategy;

public interface FlowStepStrategyFactory<T> {
  public FlowStepStrategy<T> getFlowStepStrategy();
}
