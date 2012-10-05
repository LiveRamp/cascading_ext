package com.liveramp.cascading_ext.bloom;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepStrategy;
import cascading.flow.planner.BaseFlowStep;
import cascading.scheme.hadoop.SequenceFile;
import cascading.stats.FlowStepStats;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import cascading.util.Pair;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.liveramp.cascading_ext.Bytes;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.assembly.BloomAssembly;
import com.liveramp.cascading_ext.assembly.BloomJoin;
import com.liveramp.cascading_ext.tap.TapHelper;
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
    JobConf conf = flowStep.getConfig();

    //  the job is the filter which needs to use the bloom filter
    String sourceBloomID = conf.get("source.bloom.filter.id");
    if (sourceBloomID != null) {
      buildBloomfilter(sourceBloomID, flowStep, predecessorSteps);
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
      TupleEntryIterator in = parts.openForRead(CascadingUtil.get().getFlowProcess());
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

        // This is the side bucket that the HyperLogLog writes to
        Tap approxCountsTap = new Hfs(new SequenceFile(new Fields("bytes")), stepConf.getRaw("bloom.keys.counts.dir"));

        long prevJobTuples = getApproxDistinctKeysCount(approxCountsTap);

        Pair<Double, Integer> optimal = BloomUtil.getOptimalFalsePositiveRateAndNumHashes(BloomConstants.DEFAULT_BLOOM_FILTER_BITS, prevJobTuples);
        LOG.info("Counted " + prevJobTuples + " distinct keys");
        LOG.info("Using " + BloomConstants.DEFAULT_BLOOM_FILTER_BITS + " bits in the bloom filter");
        LOG.info("Found a false positive rate of: " + optimal.getLhs());
        LOG.info("Will use " + optimal.getRhs() + " bloom hashes");

        // Extract the counters from the previous job to approximate the average key/tuple size
        FlowStepStats stats = ((BaseFlowStep) step).getFlowStepStats();

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

        LOG.info("Avg key size ~= " + avgKeySize);
        LOG.info("Avg match size ~= " + avgMatchSize);

        long splitSize = Long.parseLong(stepConf.get("split.size"));
        int numBloomHashes = optimal.getRhs();

        for (Map.Entry<String, String> entry : BloomUtil.getPropertiesForRelevance(avgMatchSize, avgKeySize).entrySet()) {
          currentStepConf.set(entry.getKey(), entry.getValue());
        }

        try {
          // Load bloom filter parts and merge them.
          Tap bloomParts = new Hfs(new SequenceFile(new Fields("split", "filter")), bloomPartsDir+"/"+(numBloomHashes-1));

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
