package com.liveramp.cascading_ext.flow_step_strategy;

import cascading.flow.FlowStepStrategy;
import org.apache.hadoop.mapred.JobConf;

/**
 * @author eddie
 */
public class SimpleFlowStepStrategyFactory implements FlowStepStrategyFactory<JobConf> {
  private final Class<? extends FlowStepStrategy<JobConf>> klass;

  public SimpleFlowStepStrategyFactory(Class<? extends FlowStepStrategy<JobConf>> klass){
    this.klass = klass;
  }

  @Override
  public FlowStepStrategy<JobConf> getFlowStepStrategy() {
    try {
      return klass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
