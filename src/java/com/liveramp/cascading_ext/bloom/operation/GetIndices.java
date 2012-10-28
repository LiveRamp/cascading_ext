package com.liveramp.cascading_ext.bloom.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.liveramp.cascading_ext.Bytes;
import com.liveramp.cascading_ext.bloom.BloomProps;
import com.liveramp.cascading_ext.bloom.BloomUtil;
import com.liveramp.cascading_ext.hash2.HashFunction;
import com.liveramp.cascading_ext.hash2.HashFunctionFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;

public class GetIndices extends BaseOperation implements Function {
  private long splitSize;
  private final HashFunctionFactory factory;
  private HashFunction function;

  public GetIndices(HashFunctionFactory factory) {
    super(1, new Fields("split", "index", "hash_num"));
    this.factory = factory;
  }

  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    JobConf conf = (JobConf) flowProcess.getConfigCopy();

    int maxHashes = BloomProps.getMaxBloomHashes(conf);
    long numBits = BloomProps.getNumBloomBits(conf);
    int numSplits = BloomProps.getNumSplits(conf);
    splitSize = BloomUtil.getSplitSize(numBits, numSplits);
    function = factory.getFunction(numBits, maxHashes);
  }

  @Override
  public void operate(FlowProcess flow, FunctionCall call) {
    byte[] bytes = Bytes.getBytes((BytesWritable) call.getArguments().getObject(0));
    long[] h = function.hash(bytes);
    for (int i = 0; i < h.length; i++) {
      Tuple tuple = new Tuple(h[i] / splitSize, h[i] % splitSize, i);
      call.getOutputCollector().add(tuple);
    }
  }
}
