package com.liveramp.cascading_ext.hash2;

import java.io.Serializable;

public abstract class HashFunctionFactory implements Serializable {
  public abstract HashFunction getFunction(long maxValue, int numHashes);
}
