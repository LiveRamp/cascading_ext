package com.liveramp.cascading_ext.combiner.lib;

public class MaxExactAggregator extends MxxExactAggregator{
  @Override
  protected Long update(Long aggregate, Long nextValue) {
    return Math.max(aggregate, nextValue);
  }

  @Override
  public Long initialize() {
    return Long.MIN_VALUE;
  }
}
