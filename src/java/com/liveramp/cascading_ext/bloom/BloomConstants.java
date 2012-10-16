package com.liveramp.cascading_ext.bloom;

import com.liveramp.cascading_ext.hash2.HashFunctionFactory;
import com.liveramp.cascading_ext.hash2.murmur.Murmur64HashFactory;

/**
 * @author eddie
 */
public class BloomConstants {
  private BloomConstants() {}

  public static int MAX_BLOOM_FILTER_HASHES = 4;
  public static int BUFFER_SIZE = 300;
  public static long DEFAULT_BLOOM_FILTER_BITS = 300L * 1024 * 1024 * 8;

  public static HashFunctionFactory DEFAULT_HASH_FACTORY = new Murmur64HashFactory();
}
