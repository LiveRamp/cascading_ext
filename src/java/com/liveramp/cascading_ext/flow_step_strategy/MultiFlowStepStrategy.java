package com.liveramp.cascading_ext.flow_step_strategy;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepStrategy;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;

public class MultiFlowStepStrategy implements FlowStepStrategy<JobConf> {
  private final List<FlowStepStrategy<JobConf>> strategies;

  public MultiFlowStepStrategy(List<FlowStepStrategy<JobConf>> strategies) {
    this.strategies = strategies;
  }

  @Override
  public void apply(Flow<JobConf> jobConfFlow, List<FlowStep<JobConf>> flowSteps, FlowStep<JobConf> jobConfFlowStep) {
    for (FlowStepStrategy<JobConf> strategy : strategies) {
      strategy.apply(jobConfFlow, flowSteps, jobConfFlowStep);
    }
  }
}
