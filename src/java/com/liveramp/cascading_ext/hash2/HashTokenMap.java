package com.liveramp.cascading_ext.hash2;

import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

public class HashTokenMap {

  private final Map<Integer, HashFunctionFactory> tokenToFactories;
  private final Map<Class<? extends HashFunction>, Integer> hashToTokens;

  public HashTokenMap(Configuration conf) {
    try{
      String hashTokens = conf.get("cascading_ext.hash.function.tokens");
      tokenToFactories = new HashMap<Integer, HashFunctionFactory>();
      hashToTokens = new HashMap<Class<? extends HashFunction>,Integer>();

      for(String pair: hashTokens.split(",")){
        String[] parts = pair.split("=");
        Integer token = Integer.parseInt(parts[0]);
        HashFunctionFactory factory = (HashFunctionFactory) Class.forName(parts[1]).newInstance();

        tokenToFactories.put(token, factory);
        hashToTokens.put(factory.getFunctionClass(), token);
      }
    }catch(Exception e){
      throw new RuntimeException("Failed to initialize token map!", e);
    }
  }

  public HashFunction getFunction(Integer token, long maxValue, int numHashes){
    return tokenToFactories.get(token).getFunction(maxValue, numHashes);
  }

  public Integer getToken(HashFunction function){
    return hashToTokens.get(function.getClass());
  }
}
