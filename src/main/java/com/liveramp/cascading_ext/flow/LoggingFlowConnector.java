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
import com.liveramp.cascading_ext.CascadingUtil;
import org.apache.hadoop.mapred.JobConf;

import java.util.Map;
import java.util.regex.Pattern;

public class LoggingFlowConnector extends HadoopFlowConnector {

  private static final Pattern CHECKPOINT_SAFE_NAME = Pattern.compile("[a-zA-Z0-9\\-_]+");

  private final FlowStepStrategy<JobConf> flowStepStrategy;
  private final String defaultFlowName;
  private final JobPersister persister;

  public LoggingFlowConnector(Map<Object, Object> properties,
                              FlowStepStrategy<JobConf> flowStepStrategy,
                              JobPersister persister) {
    this(properties, flowStepStrategy, persister, null);
  }

  public LoggingFlowConnector(Map<Object, Object> properties,
                              FlowStepStrategy<JobConf> flowStepStrategy,
                              JobPersister persister,
                              String defaultFlowName) {
    super(properties);
    this.flowStepStrategy = flowStepStrategy;
    this.defaultFlowName = defaultFlowName;
    this.persister = persister;
  }

  @Override
  public Flow connect(String name, Map<String, Tap> sources, Map<String, Tap> sinks, Map<String, Tap> traps, Pipe... tails) {
    LoggingHadoopPlanner planner = new LoggingHadoopPlanner(flowStepStrategy, getProperties(), persister);
    planner.initialize(this);

    String flowName = name != null ? name : defaultFlowName;
    FlowDef definition = new FlowDef()
        .setName(flowName)
        .addTails(tails)
        .addSources(sources)
        .addSinks(sinks)
        .addTraps(traps);

    if(getProperties().containsKey(CascadingUtil.CASCADING_RUN_ID)){

      //  cascading checkpointing fails if the job is named creatively with special chars.  Be safe for now and only allow path-friendly chars
      if(!CHECKPOINT_SAFE_NAME.matcher(flowName).matches()){
        throw new RuntimeException("Flow name "+flowName+" not compatible with checkpointing! Remove special characters from name.");
      }

      definition.setRunID((String) getProperties().get(CascadingUtil.CASCADING_RUN_ID));
    }

    return planner.buildFlow(definition);
  }
}
