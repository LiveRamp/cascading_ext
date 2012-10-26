package com.liveramp.cascading_ext.bloom;

import com.liveramp.cascading_ext.hash2.HashFunctionFactory;
import com.liveramp.cascading_ext.hash2.murmur.Murmur64HashFactory;

/**
 * @author eddie
 */
public class BloomConstants {
  public static HashFunctionFactory DEFAULT_HASH_FACTORY = new Murmur64HashFactory();
}
