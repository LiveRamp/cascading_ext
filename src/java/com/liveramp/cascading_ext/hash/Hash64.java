package com.liveramp.cascading_ext.hash;

/**
 * This class represents a common API for 64-bit hashing functions.
 */
public abstract class Hash64 {
  public static final int MURMUR_HASH64 = 0;

  /**
   * Get a singleton instance of hash function of a given type.
   *
   * @param type predefined hash type
   * @return hash function instance, or null if type is invalid
   */
  public static Hash64 getInstance(int type) {
    switch (type) {
      case MURMUR_HASH64:
        return MurmurHash64.getInstance();
      default:
        return null;
    }
  }

  /**
   * Calculate a hash using all bytes from the input argument, and
   * a seed of -1.
   *
   * @param bytes input bytes
   * @return hash value
   */
  public long hash(byte[] bytes) {
    return hash(bytes, bytes.length, -1);
  }

  /**
   * Calculate a hash using all bytes from the input argument,
   * and a provided seed value.
   *
   * @param bytes input bytes
   * @param initval seed value
   * @return hash value
   */
  public long hash(byte[] bytes, int initval) {
    return hash(bytes, bytes.length, initval);
  }

  /**
   * Calculate a hash using bytes from 0 to <code>length</code>, and
   * the provided seed value
   *
   * @param bytes input bytes
   * @param length length of the valid bytes to consider
   * @param initval seed value
   * @return hash value
   */
  public abstract long hash(byte[] bytes, int length, int initval);
}
