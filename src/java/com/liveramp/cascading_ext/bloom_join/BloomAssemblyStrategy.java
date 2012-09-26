package com.liveramp.cascading_ext.bloom_join;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepStrategy;
import cascading.flow.planner.BaseFlowStep;
import cascading.scheme.hadoop.SequenceFile;
import cascading.stats.FlowStepStats;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import cascading.util.Pair;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.liveramp.cascading_ext.Bytes;
import com.liveramp.cascading_ext.FileSystemHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
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


    //  build a bloom filter before executing the step, if necessary
    configureBloomJoin(flowStep, predecessorSteps);
  }

  /**
   * If this job involves any bloom joins, we can detect it here and configure it further.
   *
   * @param flowStep
   * @param predecessorSteps
   */
  private static void configureBloomJoin(FlowStep<JobConf> flowStep, List<FlowStep<JobConf>> predecessorSteps) {
    JobConf conf = flowStep.getConfig();

    //  the job that maps and create the bloom parts needs to know about the keys
    String targetBloomID = conf.get("target.bloom.filter.id");
    if (targetBloomID != null) {
      configureBloomFilterBuilder(targetBloomID, flowStep, predecessorSteps);
    }

    //  the job is the filter which needs to use the bloom filter
    String sourceBloomID = conf.get("source.bloom.filter.id");
    if (sourceBloomID != null) {
      buildBloomfilter(sourceBloomID, flowStep, predecessorSteps);
    }
  }

  /**
   * Configure a job that builds a bloom filter
   *
   * @param bloomID
   * @param flowStep
   * @param predecessorSteps
   */
  private static void configureBloomFilterBuilder(String bloomID, FlowStep<JobConf> flowStep, List<FlowStep<JobConf>> predecessorSteps) {
    //  look through previous steps, find the pre-bloom-one
    for (FlowStep<JobConf> previousStep : predecessorSteps) {
      JobConf prevStepConf = previousStep.getConfig();
      String preBloomID = prevStepConf.get("pre.bloom.keys.pipe");

      if (bloomID.equals(preBloomID)) {
        LOG.info("Found step before bloom filter creation map: " + preBloomID);

        // This is the side bucket that the HyperLogLog writes to
        Tap approxCountsTap = new Hfs(new SequenceFile(new Fields("bytes")), prevStepConf.getRaw("bloom.keys.counts.dir"));

        // Extract the counters from the previous job to approximate the average key/tuple size
        FlowStepStats stats = ((BaseFlowStep) previousStep).getFlowStepStats();

        long prevJobTuples = getApproxDistinctKeysCount(approxCountsTap);
        long numBloomBits = BloomConstants.DEFAULT_BLOOM_FILTER_BITS;

        Pair<Double, Integer> optimal = BloomUtil.getOptimalFalsePositiveRateAndNumHashes(numBloomBits, prevJobTuples);
        LOG.info("Counted " + prevJobTuples + " distinct keys");
        LOG.info("Using " + numBloomBits + " bits in the bloom filter");
        LOG.info("Found a false positive rate of: " + optimal.getLhs());
        LOG.info("Will use " + optimal.getRhs() + " bloom hashes");

        //  get flow step stat where flow step is equal
        JobConf stepConf = flowStep.getConfig();
        stepConf.set("num.bloom.bits", Long.toString(BloomConstants.DEFAULT_BLOOM_FILTER_BITS));
        stepConf.set("num.bloom.hashes", Integer.toString(optimal.getRhs()));
        stepConf.set("split.size", Long.toString(BloomUtil.getSplitSize(numBloomBits, BloomJoin.NUM_SPLITS)));

        // Collect some of the stats gathered. This will help configure the bloom filter
        long numSampled = stats.getCounterValue(BloomJoin.StatsCounters.TOTAL_SAMPLED_TUPLES);
        long keySizeSum = stats.getCounterValue(BloomJoin.StatsCounters.KEY_SIZE_SUM);
        long matchSizeSum = stats.getCounterValue(BloomJoin.StatsCounters.TUPLE_SIZE_SUM);

        int avgKeySize = 0;
        int avgMatchSize = 0;

        //  don't divide by zero if the filter is empty
        if (numSampled != 0) {
          avgKeySize = (int) (keySizeSum / numSampled);
          avgMatchSize = (int) (matchSizeSum / numSampled);
        }

        stepConf.set("avg.key.size", Integer.toString(avgKeySize));
        stepConf.set("avg.match.size", Integer.toString(avgMatchSize));
      }
    }
  }

  /**
   * Read from the side bucket that HyperLogLog wrote to, merge the HLL estimators, and return the
   * approximate count of distinct keys
   *
   * @param parts
   * @return
   */
  private static long getApproxDistinctKeysCount(Tap parts) {
    try {
      TupleEntryIterator in = parts.openForRead(get().getFlowProcess());
      List<HyperLogLog> countParts = new LinkedList<HyperLogLog>();

      while (in.hasNext()) {
        TupleEntry tuple = in.next();
        byte[] serializedHll = Bytes.getBytes((BytesWritable) tuple.getObject("bytes"));

        countParts.add(HyperLogLog.Builder.build(serializedHll));
      }

      ICardinality merged = new HyperLogLog(BloomAssembly.DEFAULT_ERR_PERCENTAGE).merge(countParts.toArray(new ICardinality[countParts.size()]));

      return merged.cardinality();
    } catch (IOException e) {
      throw new RuntimeException("couldn't open approximate distinct keys tap", e);
    } catch (CardinalityMergeException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Merges bloom filter parts created across multiple splits of the keys and put the result in the distributed cache.
   *
   * @param bloomID
   * @param currentStep
   * @param predecessorSteps
   */
  private static void buildBloomfilter(String bloomID, FlowStep<JobConf> currentStep, List<FlowStep<JobConf>> predecessorSteps) {
    JobConf currentStepConf = currentStep.getConfig();
    String requiredBloomPath = currentStepConf.get("required.bloom.filter.path");

    for (FlowStep<JobConf> step : predecessorSteps) {
      JobConf stepConf = step.getConfig();
      String targetBloomID = stepConf.get("target.bloom.filter.id");
      String bloomPartsDir = stepConf.get("target.bloom.filter.parts");

      if (bloomID.equals(targetBloomID)) {
        LOG.info("Found step generating required bloom filter: " + targetBloomID);
        LOG.info("Bloom filter parts located in: " + bloomPartsDir);

        long splitSize = Long.parseLong(stepConf.get("split.size"));
        int numBloomHashes = Integer.parseInt(stepConf.get("num.bloom.hashes"));

        Integer avgMatchSize = Integer.valueOf(stepConf.get("avg.match.size"));
        Integer avgKeySize = Integer.valueOf(stepConf.get("avg.key.size"));

        LOG.info("Avg key size ~= " + avgKeySize);
        LOG.info("Avg match size ~= " + avgMatchSize);

        for (Map.Entry<String, String> entry : BloomUtil.getPropertiesForRelevance(avgMatchSize, avgKeySize).entrySet()) {
          currentStepConf.set(entry.getKey(), entry.getValue());
        }

        try {
          // Load bloom filter parts and merge them.
          Tap bloomParts = new Hfs(new SequenceFile(new Fields("split", "filter")), bloomPartsDir);
          BytesBloomFilter filter = BloomUtil.mergeBloomParts(bloomParts, BloomConstants.DEFAULT_BLOOM_FILTER_BITS, splitSize, numBloomHashes);

          // Write merged bloom filter to HDFS
          LOG.info("Writing created bloom filter to FS: " + requiredBloomPath);
          filter.writeToFileSystem(FileSystemHelper.getFS(), new Path(requiredBloomPath));

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

        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
