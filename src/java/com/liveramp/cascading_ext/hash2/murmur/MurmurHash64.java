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

package com.liveramp.cascading_ext.hash2.murmur;

import com.liveramp.cascading_ext.hash2.HashFunction;

/**
 * Implementation from http://d3s.mff.cuni.cz/~holub/sw/javamurmurhash/
 */
public class MurmurHash64 extends HashFunction {

  protected MurmurHash64(long maxValue, int numHashes) {
    super(maxValue, numHashes);
  }

  @Override
  public long hash(byte[] data, int length, int seed) {
    final long m = 0xc6a4a7935bd1e995L;
    final int r = 47;

    long h = seed ^ (length * m);

    final int remainder = length & 7;
    final int end = length - remainder;
    for (int i = 0; i < end; i += 8) {
      long k = data[i + 7];
      k = k << 8;
      k = k | (data[i + 6] & 0xff);
      k = k << 8;
      k = k | (data[i + 5] & 0xff);
      k = k << 8;
      k = k | (data[i + 4] & 0xff);
      k = k << 8;
      k = k | (data[i + 3] & 0xff);
      k = k << 8;
      k = k | (data[i + 2] & 0xff);
      k = k << 8;
      k = k | (data[i + 1] & 0xff);
      k = k << 8;
      k = k | (data[i] & 0xff);

      k *= m;
      k ^= k >>> r;
      k *= m;

      h ^= k;
      h *= m;
    }

    switch (remainder) {
      case 7:
        h ^= (long) (data[end + 6] & 0xff) << 48;
      case 6:
        h ^= (long) (data[end + 5] & 0xff) << 40;
      case 5:
        h ^= (long) (data[end + 4] & 0xff) << 32;
      case 4:
        h ^= (long) (data[end + 3] & 0xff) << 24;
      case 3:
        h ^= (long) (data[end + 2] & 0xff) << 16;
      case 2:
        h ^= (long) (data[end + 1] & 0xff) << 8;
      case 1:
        h ^= (long) (data[end] & 0xff);
        h *= m;
    }

    h ^= h >>> r;
    h *= m;
    h ^= h >>> r;

    return h;
  }

  @Override
  public String getHashID() {
    return "murmur64";
  }
}
