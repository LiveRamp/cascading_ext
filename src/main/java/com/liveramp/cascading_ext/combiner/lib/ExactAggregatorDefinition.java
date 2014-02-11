/**
 *  Copyright 2012 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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
