package com.liveramp.cascading_ext.hash;

import com.liveramp.cascading_ext.bloom_join.Key;

public final class Hash64Function {
  /** The number of hashed values. */
  private int nbHash;

  /** The maximum highest returned value. */
  private long maxValue;

  /** Hashing algorithm to use. */
  private Hash64 hashFunction;

  /**
   * Constructor.
   * <p>
   * Builds a hash function that must obey to a given maximum number of returned values and a highest value.
   *
   * @param maxValue The maximum highest returned value.
   * @param nbHash The number of resulting hashed values.
   * @param hashType type of the hashing function (see {@link org.apache.hadoop.util.hash.Hash}).
   */
  public Hash64Function(long maxValue, int nbHash, int hashType) {
    if (maxValue <= 0) {
      throw new IllegalArgumentException("maxValue must be > 0");
    }

    if (nbHash <= 0) {
      throw new IllegalArgumentException("nbHash must be > 0");
    }

    this.maxValue = maxValue;
    this.nbHash = nbHash;
    this.hashFunction = Hash64.getInstance(hashType);
    if (this.hashFunction == null)
      throw new IllegalArgumentException("hashType must be known");
  }

  /** Clears <i>this</i> hash function. A NOOP */
  public void clear() {}

  /**
   * Hashes a specified key into several integers.
   *
   * @param k The specified key.
   * @return The array of hashed values.
   */
  public long[] hash(Key k) {
    byte[] b = k.getBytes();
    if (b.length == 0) {
      throw new IllegalArgumentException("key length must be > 0");
    }
    long[] result = new long[nbHash];
    int initval = -1;
    for (int i = 0; i < nbHash; i++ ) {
      long hash;
      hash = hashFunction.hash(b, initval);
      initval = (int) hash;
      result[i] = Math.abs(hash % maxValue);
    }
    return result;
  }
}
