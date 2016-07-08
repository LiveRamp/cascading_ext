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
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import com.google.common.collect.Maps;
import com.liveramp.cascading_ext.FixedSizeBitSet;
import com.liveramp.cascading_ext.bloom.BloomProps;
import com.liveramp.cascading_ext.bloom.BloomUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.Map;

public class CreateBloomFilterFromIndices extends BaseOperation<CreateBloomFilterFromIndices.Context> implements Aggregator<CreateBloomFilterFromIndices.Context> {
  private Map<Integer, TupleEntryCollector> numHashesToCollector = Maps.newHashMap();

  int minHashes;
  int maxHashes;

  public CreateBloomFilterFromIndices() {
    super(Fields.NONE);
  }

  public static class Context {
    Map<Integer, FixedSizeBitSet> numHashesToBitSet = Maps.newHashMap();
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<CreateBloomFilterFromIndices.Context> operationCall) {
    try {
      JobConf conf = (JobConf) flowProcess.getConfigCopy();
      String partsRoot = BloomProps.getBloomFilterPartsDir(conf);
      maxHashes = BloomProps.getMaxBloomHashes(conf);
      minHashes = BloomProps.getMinBloomHashes(conf);

      for (int i = minHashes; i <= maxHashes; i++) {
        Hfs tap = new Hfs(new SequenceFile(new Fields("split", "filter")), partsRoot + "/" + i + "/");
        numHashesToCollector.put(i, tap.openForWrite(new HadoopFlowProcess(conf)));
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void aggregate(FlowProcess flow, AggregatorCall<CreateBloomFilterFromIndices.Context> call) {

    Context c = call.getContext();
    long bit = (Long) call.getArguments().getObject(0);
    int hashNum = (Integer) call.getArguments().getObject(1);

    for(int i = maxHashes; i > hashNum - 1 && i >= minHashes; i--){
      c.numHashesToBitSet.get(i).set(bit);
    }
  }

  @Override
  public void complete(FlowProcess flow, AggregatorCall<CreateBloomFilterFromIndices.Context> call) {
    Context c = call.getContext();
    TupleEntry group = call.getGroup();

    for(int i = minHashes; i <= maxHashes; i++){
      numHashesToCollector.get(i).add(new Tuple(group.getObject("split"), new BytesWritable(c.numHashesToBitSet.get(i).getRaw())));
    }

  }

  @Override
  public void cleanup(FlowProcess flowProcess, OperationCall<CreateBloomFilterFromIndices.Context> operationCall) {
    for (TupleEntryCollector collector : numHashesToCollector.values()) {
      collector.close();
    }
  }

  @Override
  public void start(FlowProcess flow, AggregatorCall<CreateBloomFilterFromIndices.Context> call) {
    JobConf conf = (JobConf) flow.getConfigCopy();

    long numBits = BloomProps.getNumBloomBits(conf);
    int numSplits = BloomProps.getNumSplits(conf);
    long splitSize = BloomUtil.getSplitSize(numBits, numSplits);

    Context c = new Context();

    for(int i = minHashes; i <= maxHashes; i++){
      c.numHashesToBitSet.put(i, new FixedSizeBitSet(splitSize, new byte[FixedSizeBitSet.getNumBytesToStore(splitSize)]));
    }

    call.setContext(c);
  }
}
