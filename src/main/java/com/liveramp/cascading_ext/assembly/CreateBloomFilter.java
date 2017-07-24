/**
 * Copyright 2012 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.liveramp.cascading_ext.assembly;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Unique;
import cascading.property.ConfigDef;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.bloom.*;
import com.liveramp.cascading_ext.bloom.BloomFilter;
import com.liveramp.cascading_ext.bloom.operation.CreateBloomFilterFromIndices;
import com.liveramp.cascading_ext.bloom.operation.GetIndices;
import com.liveramp.cascading_ext.hash.HashFunctionFactory;
import com.liveramp.cascading_ext.tap.NullTap;

public class CreateBloomFilter extends SubAssembly {

  public CreateBloomFilter(Pipe keys, String bloomFilterID, String approxCountPartsDir, String bloomPartsDir, String keyBytesField) throws IOException {
    this(keys, bloomFilterID, approxCountPartsDir, bloomPartsDir, keyBytesField, HashFunctionFactory.DEFAULT_HASH_FACTORY);
  }

  public CreateBloomFilter(Pipe keys, String bloomFilterID, String approxCountPartsDir, String bloomPartsDir, String keyBytesField, HashFunctionFactory hashFactory) throws IOException {
    super(keys);

    Pipe smallPipe = new Each(keys, new Fields(keyBytesField), new GetIndices(hashFactory), new Fields("split", "index", "hash_num"));
    smallPipe = new Each(smallPipe, new Fields("split", "index", "hash_num"), new Unique.FilterPartialDuplicates());
    smallPipe = new GroupBy(smallPipe, new Fields("split"));
    smallPipe = new Every(smallPipe, new Fields("index", "hash_num"), new CreateBloomFilterFromIndices(), Fields.ALL);

    ConfigDef bloomDef = smallPipe.getStepConfigDef();
    bloomDef.setProperty(BloomProps.BLOOM_FILTER_PARTS_DIR, bloomPartsDir);
    bloomDef.setProperty(BloomProps.BLOOM_KEYS_COUNTS_DIR, approxCountPartsDir);
    bloomDef.setProperty(BloomProps.TARGET_BLOOM_FILTER_ID, bloomFilterID);

    setTails(smallPipe);
  }

  // Returns a Flow which should be executed. Once the Flow is complete, the bloomfilter can be retrieved from the Supplier
  public static Pair<Flow, Supplier<BloomFilter>> createBloomForKeys(Tap source, Pipe keys, Fields keyFields, FlowConnector connector) throws IOException {
    String bloomJobID = UUID.randomUUID().toString();
    Path bloomTempDir = FileSystemHelper.getRandomTemporaryPath("/tmp/bloom_tmp/");
    String bloomPartsDir = bloomTempDir + "/parts";
    String bloomFinalFilter = bloomTempDir + "/filter.bloomfilter";
    String approxCountPartsDir = bloomTempDir + "/approx_distinct_keys_parts/";

    // These pipes write the bloom filter to 100 part files, representing the first 1/100 of the bits in the filter, then
    // the second 1/100 etc. These splits are the concatenated in BloomAssemblyStrategy, which is required for BloomJoin
    // because otherwise we can't insert client-side code in the middle of a Flow.

    // The end result is that we run this pipe without any apprent output, then read the assembled filter from the side file
    // on HDFS and return it to the user
    Pipe filterPipe = new Each(keys, keyFields, new BloomAssembly.GetSerializedTuple());
    filterPipe = new CreateBloomFilter(filterPipe, bloomJobID, approxCountPartsDir, bloomPartsDir, "serialized-tuple-key");

    Flow flow = connector.connect(source, new NullTap(), keys);

    Supplier<BloomFilter> filterSupplier = () -> {
      Path bloomFinalFilterPath = new Path(bloomFinalFilter);
      try {
        return BloomFilter.read(
            FileSystemHelper.getFileSystemForPath(bloomFinalFilterPath),
            bloomFinalFilterPath);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };

    return Pair.of(flow, filterSupplier);
  }

  public static Pair<Flow, Supplier<BloomFilter>> createBloomForKeys(Tap source, Pipe keys, Fields keyFields) throws IOException {
    return createBloomForKeys(source, keys, keyFields, CascadingUtil.get().getFlowConnector());
  }
}
