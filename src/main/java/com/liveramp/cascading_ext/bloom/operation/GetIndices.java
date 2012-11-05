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
import com.liveramp.cascading_ext.hash.HashFunction;
import com.liveramp.cascading_ext.hash.HashFunctionFactory;
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
