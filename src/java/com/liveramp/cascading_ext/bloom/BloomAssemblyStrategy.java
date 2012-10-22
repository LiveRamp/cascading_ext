package com.liveramp.cascading_ext.bloom;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepStrategy;
import cascading.flow.planner.BaseFlowStep;
import cascading.stats.FlowStepStats;
import com.liveramp.cascading_ext.assembly.CreateBloomFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Does the following:
 * 1) Gives jobs pretty names (Cascading 2's default names are ugly random IDs)
 * 2) Does any configuration necessary for a job that involves stuff from BloomAssembly
 */
public class BloomAssemblyStrategy implements FlowStepStrategy<JobConf> {

  private static Logger LOG = Logger.getLogger(BloomAssemblyStrategy.class);

  @Override
  public void apply(Flow<JobConf> flow, List<FlowStep<JobConf>> predecessorSteps, FlowStep<JobConf> flowStep) {
    JobConf conf = flowStep.getConfig();

    //  the job is the filter which needs to use the bloom filter
    String sourceBloomID = conf.get(BloomProps.SOURCE_BLOOM_FILTER_ID);
    if (sourceBloomID != null) {
      buildBloomfilter(sourceBloomID, flowStep, predecessorSteps);
    }
  }

  /**
   * Merges bloom filter parts created across multiple splits of the keys and put the result in the distributed cache.
   *
   * @param bloomID
   * @param currentStep
   * @param predecessorSteps
   */
  private void buildBloomfilter(String bloomID, FlowStep<JobConf> currentStep, List<FlowStep<JobConf>> predecessorSteps) {
    JobConf currentStepConf = currentStep.getConfig();
    String requiredBloomPath = currentStepConf.get(BloomProps.REQUIRED_BLOOM_FILTER_PATH);

    for (FlowStep<JobConf> step : predecessorSteps) {
      JobConf stepConf = step.getConfig();
      String targetBloomID = stepConf.get(BloomProps.TARGET_BLOOM_FILTER_ID);

      if (bloomID.equals(targetBloomID)){
        LOG.info("Found step generating required bloom filter: " + targetBloomID);

        // Extract the counters from the previous job to approximate the average key/tuple size
        FlowStepStats stats = ((BaseFlowStep) step).getFlowStepStats();

        // Collect some of the stats gathered. This will help configure the bloom filter
        long numSampled = stats.getCounterValue(CreateBloomFilter.StatsCounters.TOTAL_SAMPLED_TUPLES);
        long keySizeSum = stats.getCounterValue(CreateBloomFilter.StatsCounters.KEY_SIZE_SUM);
        long matchSizeSum = stats.getCounterValue(CreateBloomFilter.StatsCounters.TUPLE_SIZE_SUM);

        int avgKeySize = 0;
        int avgMatchSize = 0;

        if (numSampled != 0) {
          avgKeySize = (int) (keySizeSum / numSampled);
          avgMatchSize = (int) (matchSizeSum / numSampled);
        }

        LOG.info("Avg key size ~= " + avgKeySize);
        LOG.info("Avg match size ~= " + avgMatchSize);

        for (Map.Entry<String, String> entry : BloomUtil.getPropertiesForRelevance(avgMatchSize, avgKeySize).entrySet()) {
          currentStepConf.set(entry.getKey(), entry.getValue());
        }

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

        BloomUtil.writeFilterToHdfs(stepConf, requiredBloomPath);
      }
    }
  }
}
