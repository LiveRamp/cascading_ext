package com.liveramp.cascading_ext.bloom;

import cascading.flow.FlowStepStrategy;
import com.liveramp.cascading_ext.flow_step_strategy.FlowStepStrategyFactory;
import com.liveramp.cascading_ext.hash2.HashFunctionFactory;
import org.apache.hadoop.mapred.JobConf;

public class BloomAssemblyStrategyFactory implements FlowStepStrategyFactory<JobConf> {

  private final HashFunctionFactory factory;
  public BloomAssemblyStrategyFactory(HashFunctionFactory factory){
    this.factory = factory;
  }

  @Override
  public FlowStepStrategy<JobConf> getFlowStepStrategy() {
    return new BloomAssemblyStrategy(factory);
  }
}
