package com.liveramp.cascading_ext.bloom.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.liveramp.cascading_ext.Bytes;
import com.liveramp.cascading_ext.bloom.BloomConstants;
import com.liveramp.cascading_ext.bloom.Key;
import com.liveramp.cascading_ext.hash.Hash64;
import com.liveramp.cascading_ext.hash.Hash64Function;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;

public class GetIndices extends BaseOperation implements Function {
  private long numBits;
  private int numHashes;
  private long splitSize;
  private final int hashType;
  private transient Hash64Function hash;

  public GetIndices() {
    this(Hash64.MURMUR_HASH64);
  }

  public GetIndices(int hashType) {
    super(1, new Fields("split", "index", "hash_num"));
    this.hashType = hashType;
  }

  public void prepare(FlowProcess flowProcess, OperationCall operationCall){
    JobConf conf = (JobConf) flowProcess.getConfigCopy();
    numBits = Long.parseLong(conf.get("num.bloom.bits"));
    numHashes = Integer.parseInt(conf.get("max.bloom.hashes"));
    splitSize = Long.parseLong(conf.get("split.size"));
  }

  @Override
  public void operate(FlowProcess flow, FunctionCall call) {
    byte[] bytes = Bytes.getBytes((BytesWritable) call.getArguments().get(0));
    if (hash == null) {
      hash = new Hash64Function(numBits, numHashes, hashType);
    }
    hash.clear();
    long[] h = hash.hash(new Key(bytes));
    for (int i = 0; i < h.length; i++ ) {
      Tuple tuple = new Tuple(h[i] / splitSize, h[i] % splitSize, i);
      call.getOutputCollector().add(tuple);
    }
  }
}
