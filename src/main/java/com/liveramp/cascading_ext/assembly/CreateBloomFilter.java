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

import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Unique;
import cascading.property.ConfigDef;
import cascading.tuple.Fields;

import com.liveramp.cascading_ext.bloom.BloomProps;
import com.liveramp.cascading_ext.bloom.operation.CreateBloomFilterFromIndices;
import com.liveramp.cascading_ext.bloom.operation.GetIndices;
import com.liveramp.cascading_ext.hash.HashFunctionFactory;

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
}
