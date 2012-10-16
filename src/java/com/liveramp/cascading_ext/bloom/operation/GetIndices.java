package com.liveramp.cascading_ext.bloom.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.liveramp.cascading_ext.Bytes;
import com.liveramp.cascading_ext.bloom.Key;
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

  public void prepare(FlowProcess flowProcess, OperationCall operationCall){
    JobConf conf = (JobConf) flowProcess.getConfigCopy();
    long numBits = Long.parseLong(conf.get("num.bloom.bits"));
    int numHashes = Integer.parseInt(conf.get("max.bloom.hashes"));
    splitSize = Long.parseLong(conf.get("split.size"));
    function = factory.getFunction(numBits, numHashes);
  }

  @Override
  public void operate(FlowProcess flow, FunctionCall call) {
    byte[] bytes = Bytes.getBytes((BytesWritable) call.getArguments().get(0));
    long[] h = function.hash(new Key(bytes));
    for (int i = 0; i < h.length; i++ ) {
      Tuple tuple = new Tuple(h[i] / splitSize, h[i] % splitSize, i);
      call.getOutputCollector().add(tuple);
    }
  }
}
