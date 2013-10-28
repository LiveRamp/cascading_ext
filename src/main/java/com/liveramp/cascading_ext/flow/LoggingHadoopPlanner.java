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

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowElement;
import cascading.flow.FlowStepStrategy;
import cascading.flow.hadoop.HadoopFlow;
import cascading.flow.hadoop.planner.HadoopPlanner;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.hadoop.util.ObjectSerializer;
import cascading.flow.planner.ElementGraph;
import cascading.operation.Operation;
import cascading.pipe.Operator;
import cascading.pipe.Pipe;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.util.HashMap;
import java.util.Map;

public class LoggingHadoopPlanner extends HadoopPlanner {
  private final FlowStepStrategy<JobConf> flowStepStrategy;
  private final Map<Object, Object> properties;
  private final JobConf jobConf;

  public LoggingHadoopPlanner(FlowStepStrategy<JobConf> flowStepStrategy, Map<Object, Object> properties) {
    super();
    this.flowStepStrategy = flowStepStrategy;
    this.properties = new HashMap<Object, Object>(properties);

    // Cache the jobconf in order to be able to instantiate custom
    // serializers. There doesn't seem to be a cleaner way to access
    // it at this point.
    this.jobConf = createJobConf(properties);
  }

  @Override
  protected HadoopFlow createFlow( FlowDef flowDef ){
    LoggingFlow flow = new LoggingFlow( getPlatformInfo(), getProperties(), getConfig(), flowDef );
    flow.setFlowStepStrategy(flowStepStrategy);
    return flow;
  }

  @Override
  protected ElementGraph createElementGraph(FlowDef flowDef, Pipe[] pipes) {
    final ElementGraph elementGraph = super.createElementGraph(flowDef, pipes);

    verifyAllOperationsAreSerializable(elementGraph);

    return elementGraph;
  }

  protected void initialize(FlowConnector flowConnector) {
    super.initialize(flowConnector, properties);
  }

  private void verifyAllOperationsAreSerializable(ElementGraph elementGraph) {
    for (FlowElement flowElement : elementGraph.vertexSet()) {
      if (flowElement instanceof Operator) {
        Operator operator = (Operator) flowElement;
        verifySingleOperationIsSerializable(operator.getOperation());
      }
    }
  }

  protected void verifySingleOperationIsSerializable(Operation operation) {
    // Checking that the operation implements Serializable is not enough since
    // it may contain a non-serializable non-transient field. Thus, we attempt
    // to serialize it and if there's an issue, we include the class name
    // of the offending operation in the error.  Otherwise it can be time
    // consuming to track down which operation is causing problems.

    try {
      ObjectSerializer objectSerializer = HadoopUtil.instantiateSerializer(jobConf, operation.getClass());
      try {
        objectSerializer.serialize(operation, true);
      } catch (ObjectStreamException e) {
        throw new RuntimeException("Could not serialize operation: " + operation.getClass().getCanonicalName(), e);
      } catch (IOException e) {
        throw new RuntimeException("Error while trying to serialize: " + operation.getClass().getCanonicalName(), e);
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
