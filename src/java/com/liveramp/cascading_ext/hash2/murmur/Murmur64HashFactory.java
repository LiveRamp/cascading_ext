package com.liveramp.cascading_ext.hash2.murmur;

import com.liveramp.cascading_ext.hash2.HashFunction;
import com.liveramp.cascading_ext.hash2.HashFunctionFactory;

public class Murmur64HashFactory extends HashFunctionFactory {
  @Override
  public HashFunction getFunction(long maxValue, int numHashes) {
    return new MurmurHash64(maxValue, numHashes);
  }
}
