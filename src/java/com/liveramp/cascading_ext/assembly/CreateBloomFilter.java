package com.liveramp.cascading_ext.assembly;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.pipe.*;
import cascading.pipe.assembly.Unique;
import cascading.property.ConfigDef;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.TupleSerializationUtil;
import com.liveramp.cascading_ext.bloom.BloomConstants;
import com.liveramp.cascading_ext.bloom.BloomUtil;
import com.liveramp.cascading_ext.bloom.operation.CreateBloomFilterFromIndices;
import com.liveramp.cascading_ext.bloom.operation.GetIndices;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

public class CreateBloomFilter extends SubAssembly {

  public enum StatsCounters {
    TOTAL_SAMPLED_TUPLES,
    KEY_SIZE_SUM,
    TUPLE_SIZE_SUM
  }

  public static final int NUM_SPLITS = 100;

  /**
   * This parameter controls how accurate (and how much memory) HyperLogLog takes to approximate the
   * distinct number of keys
   */
  public static final double DEFAULT_ERR_PERCENTAGE = 0.01;

  /**
   * To compute optimal parameters for bloom filter creation, we need to know the average key size
   * and the average tuple size on the key side. This parameter controls the rate at which we sample
   * the keys to approximate an average.
   */
  public static double DEFAULT_SAMPLE_RATE = 0.01;

  public CreateBloomFilter(Pipe keys, String approxCountPartsDir, String bloomPartsDir, String keyBytesField, Map<String, String> stepProps) throws IOException {

    Tap approxCountParts = new Hfs(new SequenceFile(new Fields("bytes")), approxCountPartsDir);
    Tap[] bloomParts = new Tap[BloomConstants.MAX_BLOOM_FILTER_HASHES];
    for(int i= 0; i < bloomParts.length; i++) {
      String dir = bloomPartsDir+"/"+i+"/";
      FileSystemHelper.safeMkdirs(FileSystemHelper.getFS(), new Path(dir));
      bloomParts[i] = new Hfs(new SequenceFile(new Fields("split", "filter")), dir);
    }

    // Collect stats used to configure the bloom filter creation step
    Pipe smallPipe = new Each(keys, new CollectKeyStats(keyBytesField, DEFAULT_SAMPLE_RATE, DEFAULT_ERR_PERCENTAGE, approxCountParts));

    smallPipe = new Each(smallPipe, new Fields(keyBytesField), new GetIndices(), new Fields("split", "index", "hash_num"));
    smallPipe = new Each(smallPipe, new Fields("split", "index", "hash_num"), new Unique.FilterPartialDuplicates());
    smallPipe = new GroupBy(smallPipe, new Fields("split"));
    smallPipe = new Every(smallPipe, new Fields("index", "hash_num"), new CreateBloomFilterFromIndices(bloomParts), Fields.ALL);

    ConfigDef bloomDef = smallPipe.getStepConfigDef();
    bloomDef.setProperty("target.bloom.filter.parts", bloomPartsDir);
    bloomDef.setProperty("bloom.keys.counts.dir", approxCountPartsDir);

    bloomDef.setProperty("mapred.reduce.tasks", Integer.toString(NUM_SPLITS));
    bloomDef.setProperty("num.bloom.bits", Long.toString(BloomConstants.DEFAULT_BLOOM_FILTER_BITS));
    bloomDef.setProperty("max.bloom.hashes", Integer.toString(BloomConstants.MAX_BLOOM_FILTER_HASHES));
    bloomDef.setProperty("split.size", Long.toString(BloomUtil.getSplitSize(BloomConstants.DEFAULT_BLOOM_FILTER_BITS, NUM_SPLITS)));
    bloomDef.setProperty("io.sort.record.percent", Double.toString(0.50));

    for(Map.Entry<String, String> prop: stepProps.entrySet()){
      bloomDef.setProperty(prop.getKey(), prop.getValue());
    }

    setTails(smallPipe);
  }

  /**
   * Used to collect the following stats about the keys in a bloom join:
   *  - approximate distinct number (uses HLL)
   *  - approximate average key size
   *  - approximate average tuple size
   */
  private static class CollectKeyStats extends BaseOperation implements Filter {
    private static Logger LOG = Logger.getLogger(CollectKeyStats.class);
    private static transient Random RAND;

    private transient HyperLogLog approxCounter;
    private transient TupleSerializationUtil tupleSerializationUtil;
    private final double errorPercent;
    private final Tap sideBucket;
    private final double sampleRate;
    private final String bytesField;

    public CollectKeyStats(String bytesField, double sampleRate, double errorPercent, Tap sideBucket) {
      this.sampleRate = sampleRate;
      this.errorPercent = errorPercent;
      this.sideBucket = sideBucket;
      this.bytesField = bytesField;
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
      TupleEntry tuple = filterCall.getArguments();

      BytesWritable key = (BytesWritable) tuple.getObject(bytesField);
      approxCounter.offer(key);

      if (getRand().nextDouble() < sampleRate) {
        countSampledStats(flowProcess, tuple, key);
      }

      return false;
    }

    private void countSampledStats(FlowProcess proc, TupleEntry tuple, BytesWritable key) {
      int tupleSize = serializeTuple(tupleSerializationUtil, tuple, Fields.ALL).length;

      proc.increment(StatsCounters.TOTAL_SAMPLED_TUPLES, 1);
      proc.increment(StatsCounters.KEY_SIZE_SUM, key.getLength());
      proc.increment(StatsCounters.TUPLE_SIZE_SUM, tupleSize);
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
      super.prepare(flowProcess, operationCall);
      approxCounter = new HyperLogLog(errorPercent);
      tupleSerializationUtil = new TupleSerializationUtil((JobConf) flowProcess.getConfigCopy());
    }

    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
      super.cleanup(flowProcess, operationCall);

      try {
        LOG.info("HLL counter found "+approxCounter.cardinality()+" distinct keys");
        TupleEntryCollector out = sideBucket.openForWrite(flowProcess);
        out.add(new Tuple(new BytesWritable(approxCounter.getBytes())));
        out.close();
      } catch (IOException e) {
        throw new RuntimeException("couldn't write approximate counts to side bucket", e);
      }
    }

    protected static Random getRand() {
      if (RAND == null) {
        RAND = new Random();
      }

      return RAND;
    }
  }

  protected static byte[] serializeTuple(TupleSerializationUtil tupleSerializationUtil, TupleEntry tupleEntry, Fields selectedFields){
    try {
      if (selectedFields == Fields.ALL) {
        return tupleSerializationUtil.serialize(tupleEntry.getTuple());
      }

      Tuple value = new Tuple();

      for (Comparable field : selectedFields) {
        value.add(tupleEntry.getObject(field));
      }

      return tupleSerializationUtil.serialize(value);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
