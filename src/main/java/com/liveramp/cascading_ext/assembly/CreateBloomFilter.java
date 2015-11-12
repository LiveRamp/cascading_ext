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

package com.liveramp.cascading_ext.assembly;

import java.io.IOException;
import java.util.Random;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Unique;
import cascading.property.ConfigDef;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.liveramp.cascading_ext.bloom.BloomProps;
import com.liveramp.cascading_ext.bloom.operation.CreateBloomFilterFromIndices;
import com.liveramp.cascading_ext.bloom.operation.GetIndices;
import com.liveramp.cascading_ext.hash.HashFunctionFactory;

public class CreateBloomFilter extends SubAssembly {

  public CreateBloomFilter(Pipe keys, String bloomFilterID, String approxCountPartsDir, String bloomPartsDir, String keyBytesField) throws IOException {
    super(keys);

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
    private static Logger LOG = LoggerFactory.getLogger(CollectKeyStats.class);
    private static transient Random RAND;

    private transient HyperLogLog approxCounter;
    private final String bytesField;

    public CollectKeyStats(String bytesField) {
      this.bytesField = bytesField;
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
      TupleEntry tuple = filterCall.getArguments();

      BytesWritable key = (BytesWritable) tuple.getObject(bytesField);
      approxCounter.offer(key);

      return false;
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
      JobConf conf = (JobConf) flowProcess.getConfigCopy();
      approxCounter = new HyperLogLog(BloomProps.getHllErr(conf));
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

    protected static Random getRand() {
      if (RAND == null) {
        RAND = new Random();
      }

      return RAND;
    }
  }
}
