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

import cascading.flow.FlowStepStrategy;
import org.apache.hadoop.mapred.JobConf;

/**
 * @author eddie
 */
public class SimpleFlowStepStrategyFactory implements FlowStepStrategyFactory<JobConf> {
  private final Class<? extends FlowStepStrategy<JobConf>> klass;

  public SimpleFlowStepStrategyFactory(Class<? extends FlowStepStrategy<JobConf>> klass) {
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
