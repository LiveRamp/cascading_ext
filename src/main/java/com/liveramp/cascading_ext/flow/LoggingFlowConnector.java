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
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import org.apache.hadoop.mapred.JobConf;

import java.util.HashMap;
import java.util.Map;

public class LoggingFlowConnector extends HadoopFlowConnector {
  private final FlowStepStrategy<JobConf> flowStepStrategy;

  public LoggingFlowConnector(Map<Object, Object> properties, FlowStepStrategy<JobConf> flowStepStrategy) {
    super(properties);
    this.flowStepStrategy = flowStepStrategy;
  }

  @Override
  public Flow connect(String name, Map<String, Tap> sources, Map<String, Tap> sinks, Map<String, Tap> traps, Pipe... tails) {
    LoggingHadoopPlanner planner = new LoggingHadoopPlanner(flowStepStrategy);
    planner.initialize(this, new HashMap<Object, Object>(getProperties()));

    return planner
        .buildFlow(new FlowDef()
            .setName(name)
            .addTails(tails)
            .addSources(sources)
            .addSinks(sinks)
            .addTraps(traps));
  }
}
