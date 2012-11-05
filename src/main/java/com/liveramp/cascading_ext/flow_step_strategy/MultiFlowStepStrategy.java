/**
 *  Copyright 2012 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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
