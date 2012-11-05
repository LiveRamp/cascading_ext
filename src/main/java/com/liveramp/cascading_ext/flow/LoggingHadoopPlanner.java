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

package com.liveramp.cascading_ext.flow;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowStepStrategy;
import cascading.flow.hadoop.planner.HadoopPlanner;
import org.apache.hadoop.mapred.JobConf;

public class LoggingHadoopPlanner extends HadoopPlanner {
  private final FlowStepStrategy<JobConf> flowStepStrategy;

  public LoggingHadoopPlanner(FlowStepStrategy<JobConf> flowStepStrategy) {
    super();
    this.flowStepStrategy = flowStepStrategy;
  }

  @Override
  public Flow buildFlow(FlowDef flowDef) {
    Flow<JobConf> internalFlow = super.buildFlow(flowDef);
    internalFlow.setFlowStepStrategy(flowStepStrategy);
    return new LoggingFlow(internalFlow);
  }
}
