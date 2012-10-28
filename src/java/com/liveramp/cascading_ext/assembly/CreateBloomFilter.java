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
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.liveramp.cascading_ext.TupleSerializationUtil;
import com.liveramp.cascading_ext.bloom.BloomProps;
import com.liveramp.cascading_ext.bloom.operation.CreateBloomFilterFromIndices;
import com.liveramp.cascading_ext.bloom.operation.GetIndices;
import com.liveramp.cascading_ext.hash2.HashFunctionFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Random;

public class CreateBloomFilter extends SubAssembly {

  public enum StatsCounters {
    TOTAL_SAMPLED_TUPLES,
    KEY_SIZE_SUM,
    TUPLE_SIZE_SUM
  }

  public CreateBloomFilter(Pipe keys, String bloomFilterID, String approxCountPartsDir, String bloomPartsDir, String keyBytesField) throws IOException {

    // Collect stats used to configure the bloom filter creation step
    Pipe smallPipe = new Each(keys, new CollectKeyStats(keyBytesField));

    smallPipe = new Each(smallPipe, new Fields(keyBytesField), new GetIndices(HashFunctionFactory.DEFAULT_HASH_FACTORY), new Fields("split", "index", "hash_num"));
    smallPipe = new Each(smallPipe, new Fields("split", "index", "hash_num"), new Unique.FilterPartialDuplicates());
    smallPipe = new GroupBy(smallPipe, new Fields("split"));
    smallPipe = new Every(smallPipe, new Fields("index", "hash_num"), new CreateBloomFilterFromIndices(), Fields.ALL);

    ConfigDef bloomDef = smallPipe.getStepConfigDef();
    bloomDef.setProperty(BloomProps.BLOOM_FILTER_PARTS_DIR, bloomPartsDir);
    bloomDef.setProperty(BloomProps.BLOOM_KEYS_COUNTS_DIR, approxCountPartsDir);
    bloomDef.setProperty(BloomProps.TARGET_BLOOM_FILTER_ID, bloomFilterID);

    setTails(smallPipe);
  }

  /**
   * Used to collect the following stats about the keys in a bloom join:
   * - approximate distinct number (uses HLL)
   * - approximate average key size
   * - approximate average tuple size
   */
  private static class CollectKeyStats extends BaseOperation implements Filter {
    private static Logger LOG = Logger.getLogger(CollectKeyStats.class);
    private static transient Random RAND;

    private transient HyperLogLog approxCounter;
    private transient TupleSerializationUtil tupleSerializationUtil;
    private double sampleRate;
    private final String bytesField;

    public CollectKeyStats(String bytesField) {
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

      JobConf conf = (JobConf) flowProcess.getConfigCopy();
      approxCounter = new HyperLogLog(BloomProps.getHllErr(conf));
      sampleRate = BloomProps.getKeySampleRate(conf);
      tupleSerializationUtil = new TupleSerializationUtil((JobConf) flowProcess.getConfigCopy());
    }

    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
      super.cleanup(flowProcess, operationCall);
      JobConf conf = (JobConf) flowProcess.getConfigCopy();

      try {
        LOG.info("HLL counter found " + approxCounter.cardinality() + " distinct keys");

        String approxCountPartsDir = BloomProps.getApproxCountsDir(conf);
        TupleEntryCollector out = new Hfs(new SequenceFile(new Fields("bytes")), approxCountPartsDir)
            .openForWrite(flowProcess);
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

  protected static byte[] serializeTuple(TupleSerializationUtil tupleSerializationUtil, TupleEntry tupleEntry, Fields selectedFields) {
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
