package com.liveramp.cascading_ext.combiner.lib;

public class MinExactAggregator extends MxxExactAggregator{
  @Override
  protected Long update(Long aggregate, Long nextValue) {
    return Math.min(aggregate, nextValue);
  }

  @Override
  public Long initialize() {
    return Long.MAX_VALUE;
  }
}
