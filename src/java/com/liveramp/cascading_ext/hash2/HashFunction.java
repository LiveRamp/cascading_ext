package com.liveramp.cascading_ext.hash2;

import com.liveramp.cascading_ext.bloom.Key;

import java.io.Serializable;

public abstract class HashFunction implements Serializable {

  private final long maxValue;
  private final int numHashes;

  public HashFunction(long maxValue, int numHashes){
    this.maxValue = maxValue;
    this.numHashes = numHashes;
  }

  public long[] hash(Key k) {
    byte[] b = k.getBytes();
    if (b.length == 0) {
      throw new IllegalArgumentException("key length must be > 0");
    }

    long[] result = new long[numHashes];
    int initval = -1;
    for (int i = 0; i < numHashes; i++ ) {
      long hash;
      hash = hash(b, b.length, initval);
      initval = (int) hash;
      result[i] = Math.abs(hash % maxValue);
    }
    return result;
  }

  public abstract long hash(byte[] data, int length, int seed);

  public abstract String getHashID();
}
