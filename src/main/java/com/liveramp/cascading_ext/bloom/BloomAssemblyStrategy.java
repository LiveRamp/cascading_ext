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

package com.liveramp.cascading_ext.bloom;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepStrategy;

/**
 * Does any configuration necessary for a job that involves stuff from BloomAssembly
 */
public class BloomAssemblyStrategy implements FlowStepStrategy<JobConf> {

  private static Logger LOG = LoggerFactory.getLogger(BloomAssemblyStrategy.class);

  @Override
  public void apply(Flow<JobConf> flow, List<FlowStep<JobConf>> predecessorSteps, FlowStep<JobConf> flowStep) {
    JobConf conf = flowStep.getConfig();

    String targetBloomID = conf.get(BloomProps.TARGET_BLOOM_FILTER_ID);
    if (targetBloomID != null) {
      prepareBloomFilterBuilder(flowStep);
    }
    //  the job is the filter which needs to use the bloom filter
    String sourceBloomID = conf.get(BloomProps.SOURCE_BLOOM_FILTER_ID);
    if (sourceBloomID != null) {
      buildBloomfilter(sourceBloomID, flowStep, predecessorSteps);
    }

  }

  private void prepareBloomFilterBuilder(FlowStep<JobConf> currentStep) {
    JobConf currentStepConf = currentStep.getConfig();
    currentStepConf.set("mapred.reduce.tasks", Integer.toString(BloomProps.getNumSplits(currentStepConf)));
  }

  /**
   * Merges bloom filter parts created across multiple splits of the keys and put the result in the distributed cache.
   */
  private void buildBloomfilter(String bloomID, FlowStep<JobConf> currentStep, List<FlowStep<JobConf>> predecessorSteps) {
    try {
      JobConf currentStepConf = currentStep.getConfig();
      currentStepConf.set("mapred.job.reuse.jvm.num.tasks", "-1");

      String requiredBloomPath = currentStepConf.get(BloomProps.REQUIRED_BLOOM_FILTER_PATH);

      for (FlowStep<JobConf> step : predecessorSteps) {
        JobConf prevStepConf = step.getConfig();
        String targetBloomID = prevStepConf.get(BloomProps.TARGET_BLOOM_FILTER_ID);

        if (bloomID.equals(targetBloomID)) {
          LOG.info("Found step generating required bloom filter: " + targetBloomID);

          // Put merged result in distributed cache
          LOG.info("Adding dist cache properties to config:");
          for (Map.Entry<String, String> prop : BloomUtil.getPropertiesForDistCache(requiredBloomPath).entrySet()) {
            LOG.info(prop.getKey() + " = " + prop.getValue());
            String previousProperty = currentStepConf.get(prop.getKey());
            if (previousProperty != null) {
              LOG.info("found already existing value for key: " + prop.getKey() + ", found " + previousProperty + ".  Appending.");
              currentStepConf.set(prop.getKey(), previousProperty + "," + prop.getValue());
            } else {
              currentStepConf.set(prop.getKey(), prop.getValue());
            }
          }

          BloomUtil.writeFilterToHdfs(prevStepConf, requiredBloomPath);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to create bloom filter!", e);
    }
  }
}
