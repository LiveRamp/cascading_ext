package com.liveramp.cascading_ext.bloom_join;

/**
 * @author eddie
 */
public class BloomConstants {
  private BloomConstants() {}

  public static int MAX_BLOOM_FILTER_HASHES = 4;
  public static int BUFFER_SIZE = 300;
  public static long DEFAULT_BLOOM_FILTER_BITS = 300L * 1024 * 1024 * 8;
}
