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

import java.io.IOException;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.liveramp.cascading_ext.Bytes;
import com.liveramp.cascading_ext.bloom.BloomProps;
import com.liveramp.cascading_ext.bloom.BloomUtil;
import com.liveramp.cascading_ext.hash.HashFunction;
import com.liveramp.cascading_ext.hash.HashFunctionFactory;

public class GetIndices extends BaseOperation implements Function {
  private static Logger LOG = LoggerFactory.getLogger(GetIndices.class);

  private long splitSize;
  private final HashFunctionFactory factory;
  private HashFunction function;
  private long[] hashResult;

  private transient HyperLogLog approxCounter;


  public GetIndices(HashFunctionFactory factory) {
    super(1, new Fields("split", "index", "hash_num"));
    this.factory = factory;
  }

  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    JobConf conf = (JobConf)flowProcess.getConfigCopy();

    int maxHashes = BloomProps.getMaxBloomHashes(conf);
    long numBits = BloomProps.getNumBloomBits(conf);
    int numSplits = BloomProps.getNumSplits(conf);
    splitSize = BloomUtil.getSplitSize(numBits, numSplits);
    function = factory.getFunction(numBits, maxHashes);
    approxCounter = new HyperLogLog(BloomProps.getHllErr(conf));
    hashResult = new long[maxHashes];

  }

  @Override
  public void operate(FlowProcess flow, FunctionCall call) {
    byte[] bytes = Bytes.getBytes((BytesWritable)call.getArguments().getObject(0));

    approxCounter.offer(bytes);

    function.hash(bytes, hashResult);
    for (int i = 0; i < hashResult.length; i++) {
      Tuple tuple = new Tuple(hashResult[i] / splitSize, hashResult[i] % splitSize, i);
      call.getOutputCollector().add(tuple);
    }
  }

  @Override
  public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
    JobConf conf = (JobConf) flowProcess.getConfigCopy();

    try {
      LOG.info("HLL counter found " + approxCounter.cardinality() + " distinct keys");

      Hfs tap = new Hfs(new SequenceFile(new Fields("bytes")), BloomProps.getApproxCountsDir(conf));
      TupleEntryCollector out = tap.openForWrite(new HadoopFlowProcess(conf));
      out.add(new Tuple(new BytesWritable(approxCounter.getBytes())));
      out.close();

    } catch (IOException e) {
      throw new RuntimeException("couldn't write approximate counts to side bucket", e);
    }
  }

}
