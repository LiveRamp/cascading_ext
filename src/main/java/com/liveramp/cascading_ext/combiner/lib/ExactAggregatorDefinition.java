package com.liveramp.cascading_ext.combiner.lib;

import java.io.Serializable;

import cascading.tuple.Fields;

import com.liveramp.cascading_ext.combiner.ExactAggregator;
import com.liveramp.commons.util.MemoryUsageEstimator;

public class ExactAggregatorDefinition implements Serializable {

  private final Fields inputFields;
  private final Fields intermediateFields;
  private final ExactAggregator aggregator;
  private final MemoryUsageEstimator valueSizeEstimator;

  public ExactAggregatorDefinition(Fields inputFields,
      Fields intermediateFields,
      ExactAggregator aggregator, 
      MemoryUsageEstimator valueSizeEstimator) {
    this.inputFields = inputFields;
    this.intermediateFields = intermediateFields;
    this.aggregator = aggregator;
    this.valueSizeEstimator = valueSizeEstimator;
  }
  
  public ExactAggregatorDefinition(Fields inputFields,
      Fields intermediateFields,
      ExactAggregator aggregator) {
    this(inputFields, intermediateFields, aggregator, null);
  }

  public Fields getInputFields() {
    return inputFields;
  }

  public Fields getIntermediateFields() {
    return intermediateFields;
  }

  public ExactAggregator getAggregator() {
    return aggregator;
  }
  
  public MemoryUsageEstimator getValueSizeEstimator() {
    return valueSizeEstimator;
  }
}
