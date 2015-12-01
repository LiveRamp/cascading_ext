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

package com.liveramp.cascading_ext.hash;

import java.io.Serializable;

public abstract class HashFunction implements Serializable {

  private final long maxValue;
  private final int numHashes;

  public HashFunction(long maxValue, int numHashes) {
    this.maxValue = maxValue;
    this.numHashes = numHashes;
  }

  public long[] hash(byte[] b) {
    long[] result = new long[numHashes];
    hash(b, result);
    return result;
  }

    public void hash(byte[] b, long[] result) {
    if (b.length == 0) {
      throw new IllegalArgumentException("key length must be > 0");
    }

    int initval = -1;
    for (int i = 0; i < numHashes; i++) {
      long hash;
      hash = hash(b, b.length, initval);
      initval = (int) hash;
      result[i] = Math.abs(hash % maxValue);
    }
  }

  public abstract long hash(byte[] data, int length, int seed);

  public abstract String getHashID();
}
