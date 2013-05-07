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

package com.liveramp.cascading_ext.hash.murmur;

import com.google.common.hash.Hashing;
import com.liveramp.cascading_ext.hash.HashFunction;

public class MurmurHash128 extends HashFunction {

  protected MurmurHash128(long maxValue, int numHashes) {
    super(maxValue, numHashes);
  }

  @Override
  public long hash(byte[] data, int length, int seed) {
    return Hashing.murmur3_128(seed).hashBytes(data).asLong();
  }

  @Override
  public String getHashID() {
    return "murmur128";
  }
}
