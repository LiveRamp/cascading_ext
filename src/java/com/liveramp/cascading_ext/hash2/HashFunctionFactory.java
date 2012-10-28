package com.liveramp.cascading_ext.hash2;

import com.liveramp.cascading_ext.hash2.murmur.Murmur64HashFactory;

import java.io.Serializable;

public abstract class HashFunctionFactory implements Serializable {
  public static final HashFunctionFactory DEFAULT_HASH_FACTORY = new Murmur64HashFactory();

  public abstract HashFunction getFunction(long maxValue, int numHashes);
}
