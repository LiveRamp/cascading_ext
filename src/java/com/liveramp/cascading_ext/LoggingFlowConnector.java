package com.liveramp.cascading_ext;

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

  public LoggingFlowConnector(Map<Object, Object> properties, FlowStepStrategy<JobConf> flowStepStrategy){
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