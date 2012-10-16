package com.liveramp.cascading_ext.assembly;

import cascading.flow.FlowProcess;
import cascading.flow.planner.Scope;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.operation.OperationCall;
import cascading.pipe.*;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.Joiner;
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
import com.liveramp.cascading_ext.bloom.BytesBloomFilterLoader;
import com.liveramp.cascading_ext.bloom.operation.BloomJoinFilter;
import com.liveramp.cascading_ext.bloom.operation.CreateBloomFilterFromIndices;
import com.liveramp.cascading_ext.bloom.operation.GetIndices;
import com.liveramp.cascading_ext.joiner.LimitJoin;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * This SubAssembly is used by BloomJoin and BloomFilter. It builds a bloom filter from the RHS, filters
 * the LHS using the bloom filter and, depending on parameters, does a CoGroup for exactness.
 *
 */
public abstract class BloomAssembly extends SubAssembly {
  private static Logger LOG = Logger.getLogger(BloomAssembly.class);

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

  public enum StatsCounters {
    TOTAL_SAMPLED_TUPLES,
    KEY_SIZE_SUM,
    TUPLE_SIZE_SUM
  }

  protected enum Mode {
    FILTER_EXACT, FILTER_INEXACT, JOIN
  }

  public static enum CoGroupOrder {
    LARGE_LHS, LARGE_RHS
  }

  /**
   * The LHS ("largePipe") should be the side with relatively more tuples, as we will build the bloom filter against the
   * RHS ("smallPipe").
   *
   * @param largePipe the left hand side of the bloom operation. this should be the side that the bloom filter will be
   *            run against. In other words, this should be the side with relatively more tuples.
   * @param largeJoinFields the fields on the left hand side that will be compared with the right hand side
   * @param smallPipe the right hand side of the bloom operation. this should be the side that we build the bloom filter
   *            against. In other words, this should be the side with relatively fewer tuples.
   * @param smallJoinFields the fields on the right hand side that will be compared with the left hand side
   * @param renameFields if doing a join and the LHS and RHS have some of the same field names, you'll need to supply
   *                     rename fields (just like with CoGroup).
   * @param operationType controls how the bloom operation behaves
   * @param coGroupOrder determines which pipe ends up on the LHS of the final cogroup
   */
  protected BloomAssembly(Pipe largePipe, Fields largeJoinFields,
                          Pipe smallPipe, Fields smallJoinFields,
                          Fields renameFields, Mode operationType,
                          Joiner joiner, CoGroupOrder coGroupOrder){

    try {

      // Some of these parameters are used by the RapleafFlowStepStrategy to inform later steps in the bloom operation.
      String bloomJobID = UUID.randomUUID().toString();
      Path bloomTempDir = FileSystemHelper.getRandomTemporaryPath("/tmp/bloom_tmp/");

      String bloomPartsDir = bloomTempDir+"/parts";
      String bloomFinalFilter = bloomTempDir+"/filter.bloomfilter";
      String approxCountPartsDir = bloomTempDir + "/approx_distinct_keys_parts/";

      FileSystemHelper.safeMkdirs(FileSystemHelper.getFS(), new Path(approxCountPartsDir));


      Tap approxCountParts = new Hfs(new SequenceFile(new Fields("bytes")), approxCountPartsDir);
      Tap[] bloomParts = new Tap[BloomConstants.MAX_BLOOM_FILTER_HASHES];
      for(int i= 0; i < bloomParts.length; i++) {
        String dir = bloomPartsDir+"/"+i+"/";
        FileSystemHelper.safeMkdirs(FileSystemHelper.getFS(), new Path(dir));
        bloomParts[i] = new Hfs(new SequenceFile(new Fields("split", "filter")), dir);
      }

      //  if it's a filter, we care about nothing except the join keys on the RHS -- remove the rest
      if(operationType != Mode.JOIN){
        smallPipe = new Each(smallPipe, smallJoinFields, new Identity());
      }
      Pipe rhsOrig = new Pipe("smallPipe-orig", smallPipe);

      // Collect stats used to configure the bloom filter creation step
      smallPipe = new Each(smallPipe, new CollectKeyStats(DEFAULT_SAMPLE_RATE, DEFAULT_ERR_PERCENTAGE, smallJoinFields, approxCountParts));

      // This creates the bloom filter on each of the splits. Later steps merge the parts
      smallPipe = new Each(smallPipe, smallJoinFields, new GetSerializedTuple());
      smallPipe = new Each(smallPipe, new Fields("serialized"), new GetIndices(BloomConstants.DEFAULT_HASH_FACTORY), new Fields("split", "index", "hash_num"));
      smallPipe = new Each(smallPipe, new Fields("split", "index", "hash_num"), new Unique.FilterPartialDuplicates());
      smallPipe = new GroupBy(smallPipe, new Fields("split"));
      smallPipe = new Every(smallPipe, new Fields("index", "hash_num"), new CreateBloomFilterFromIndices(bloomParts), Fields.ALL);

      ConfigDef bloomDef = smallPipe.getStepConfigDef();
      bloomDef.setProperty("target.bloom.filter.id", bloomJobID);
      bloomDef.setProperty("target.bloom.filter.parts", bloomPartsDir);
      bloomDef.setProperty("bloom.keys.counts.dir", approxCountPartsDir);

      bloomDef.setProperty("mapred.reduce.tasks", Integer.toString(NUM_SPLITS));
      bloomDef.setProperty("num.bloom.bits", Long.toString(BloomConstants.DEFAULT_BLOOM_FILTER_BITS));
      bloomDef.setProperty("max.bloom.hashes", Integer.toString(BloomConstants.MAX_BLOOM_FILTER_HASHES));
      bloomDef.setProperty("split.size", Long.toString(BloomUtil.getSplitSize(BloomConstants.DEFAULT_BLOOM_FILTER_BITS, BloomJoin.NUM_SPLITS)));
      bloomDef.setProperty("io.sort.record.percent", Double.toString(0.50));

      // This is a bit of a hack to:
      //  1) Force a dependency on the operations performed on RHS above (can't continue until they're done)
      //  2) Bind RHS to the flow, which wouldn't happen otherwise.
      // Note that RHS has no output, so there shouldn't be any danger in doing this.
      Pipe filterPipe = new NaiveMerge(largePipe.getName(), largePipe, smallPipe);

      // Load the bloom filter into memory and apply it to the LHS.
      filterPipe = new Each(filterPipe, largeJoinFields,
              new BloomJoinFilter(new BytesBloomFilterLoader(), bloomJobID, false));

      // Add some config parameters that will allow CascadingHelper$RapleafFlowStepStrategy to detect that this job
      // needs the bloom filter. It will merge the bloom filter parts created previously and put the result in the
      // distributed cache.
      ConfigDef config = filterPipe.getStepConfigDef();
      config.setProperty("source.bloom.filter.id", bloomJobID);
      config.setProperty("required.bloom.filter.path", bloomFinalFilter);
      config.setProperty("mapred.job.reuse.jvm.num.tasks", "-1");

      if(operationType == Mode.FILTER_EXACT){
        // We don't actually care about the fields on the RHS (the user just expects the LHS fields), so we can
        // rename them to junk to avoid conflicts with field names on the LHS
        Fields newKeyFields = new Fields();
        for(int i = 0; i < smallJoinFields.size(); i++){
          newKeyFields = newKeyFields.append(new Fields("__bloom_join_tmp_" + i));
        }
        rhsOrig = new Each(rhsOrig, smallJoinFields, new Identity(newKeyFields), newKeyFields);

        // Limit join takes the LHS exactly once if and only if there is a match on the RHS. This is similar to
        // an InnerJoin, except it won't duplicate tuples on the LHS if there are duplicates on the RHS.
        filterPipe = getCoGroup(filterPipe, largeJoinFields, rhsOrig, newKeyFields, renameFields, joiner, coGroupOrder, operationType);

        // Get rid of the junk RHS fields
        filterPipe = new Discard(filterPipe, newKeyFields);
      }else if(operationType == Mode.JOIN){
        filterPipe = getCoGroup(filterPipe, largeJoinFields, rhsOrig, smallJoinFields, renameFields, joiner, coGroupOrder, operationType);
      }

      setTails(filterPipe);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Pipe getCoGroup(Pipe filtered, Fields largeJoinFields,
                          Pipe keysOrig, Fields keyFields,
                          Fields renameFields,
                          Joiner joinerInput,
                          CoGroupOrder coGroupOrder,
                          Mode mode){
    Pipe lhs, rhs;
    Fields lhsFields, rhsFields;
    Joiner joiner;

    if(coGroupOrder == CoGroupOrder.LARGE_LHS){
      lhs = filtered;
      lhsFields = largeJoinFields;
      rhs = keysOrig;
      rhsFields = keyFields;
      if(mode == Mode.FILTER_EXACT){
        joiner = new LimitJoin(new long[]{Long.MAX_VALUE, 1l});
      }else{
        if(joinerInput != null){
          joiner = joinerInput;
        }else{
          joiner = new InnerJoin();
        }
      }
    }else{
      lhs = keysOrig;
      lhsFields = keyFields;
      rhs = filtered;
      rhsFields = largeJoinFields;
      if(mode == Mode.FILTER_EXACT){
        joiner = new LimitJoin(new long[]{1l, Long.MAX_VALUE});
      }else{
        if(joinerInput != null){
          joiner = joinerInput;
        }else{
          joiner = new InnerJoin();
        }
      }
    }

    if(renameFields != null){
      return new CoGroup(lhs, lhsFields, rhs, rhsFields, renameFields, joiner);
    }else{
      return new CoGroup(lhs, lhsFields, rhs, rhsFields, joiner);
    }
  }

  private static class NaiveMerge extends Merge {
    String toAccept;

    public NaiveMerge(String toAccept, Pipe... pipes){
      super( null, pipes );
      this.toAccept = toAccept;
    }

    @Override
    public Scope outgoingScopeFor(Set<Scope> incomingScopes ){
      Scope toUse = null;
      for(Scope s: incomingScopes){
        if(s.getName().equals(toAccept)){
          toUse = s;
        }
      }
      return new Scope(super.outgoingScopeFor(Collections.singleton(toUse)));
    }
  }

  private static class GetSerializedTuple extends BaseOperation implements Function {

    private transient TupleSerializationUtil tupleSerializationUtil;

    public GetSerializedTuple(){
      super(new Fields("serialized"));
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
      tupleSerializationUtil = new TupleSerializationUtil((JobConf) flowProcess.getConfigCopy());
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
      try {
        Tuple tuple = functionCall.getArguments().getTuple();
        byte[] tupleSerialized = tupleSerializationUtil.serialize(tuple);
        functionCall.getOutputCollector().add(new Tuple(new BytesWritable(tupleSerialized)));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
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
    private final Fields key;
    private final double sampleRate;

    public CollectKeyStats(double sampleRate, double errorPercent, Fields key, Tap sideBucket) {
      this.sampleRate = sampleRate;
      this.errorPercent = errorPercent;
      this.key = key;
      this.sideBucket = sideBucket;
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
      TupleEntry tuple = filterCall.getArguments();

      BytesWritable key = getKeyFromTuple(tuple);
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

    public BytesWritable getKeyFromTuple(TupleEntry tuple) {
      return new BytesWritable(serializeTuple(tupleSerializationUtil, tuple, this.key));
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
