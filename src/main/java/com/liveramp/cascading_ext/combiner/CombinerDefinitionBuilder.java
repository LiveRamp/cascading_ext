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

package com.liveramp.cascading_ext.combiner;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.liveramp.commons.collections.MemoryBoundLruHashMap;
import com.liveramp.commons.util.MemoryUsageEstimator;

public class CombinerDefinitionBuilder<T> {

  private PartialAggregator<T> partialAggregator;
  private FinalAggregator<T> finalAggregator;
  private Fields groupFields;
  private Fields inputFields;
  private Fields intermediateFields;
  private Fields outputFields;
  private int limit = Combiner.DEFAULT_LIMIT;
  private long memoryLimit = MemoryBoundLruHashMap.UNLIMITED_MEMORY_CAPACITY;
  private MemoryUsageEstimator<Tuple> keySizeEstimator;
  private MemoryUsageEstimator<T> valueSizeEstimator;
  private boolean strict = Combiner.DEFAULT_STRICTNESS;
  private Evictor<T> evictor = new DontEvict<T>();
  private String name;
  private boolean keepNullGroups = true;

  public CombinerDefinitionBuilder<T> setPartialAggregator(PartialAggregator<T> partialAggregator) {
    this.partialAggregator = partialAggregator;
    return this;
  }

  public CombinerDefinitionBuilder<T> setFinalAggregator(FinalAggregator<T> finalAggregator) {
    this.finalAggregator = finalAggregator;
    return this;
  }

  public CombinerDefinitionBuilder<T> setExactAggregator(ExactAggregator<T> exactAggregator) {
    this.finalAggregator = exactAggregator;
    this.partialAggregator = exactAggregator;
    return this;
  }

  public CombinerDefinitionBuilder<T> setGroupFields(Fields groupFields) {
    this.groupFields = groupFields;
    return this;
  }

  public CombinerDefinitionBuilder<T> setInputFields(Fields inputFields) {
    this.inputFields = inputFields;
    return this;
  }

  public CombinerDefinitionBuilder<T> setIntermediateFields(Fields intermediateFields) {
    this.intermediateFields = intermediateFields;
    return this;
  }

  public CombinerDefinitionBuilder<T> setOutputFields(Fields outputFields) {
    this.outputFields = outputFields;
    return this;
  }

  public CombinerDefinitionBuilder<T> setName(String name) {
    this.name = name;
    return this;
  }

  public CombinerDefinitionBuilder<T> setLimit(int limit) {
    this.limit = limit;
    return this;
  }

  public CombinerDefinitionBuilder<T> setMemoryLimit(long numBytes) {
    this.memoryLimit = numBytes;
    return this;
  }

  public CombinerDefinitionBuilder<T> setMemoryLimitMB(long numMegabytes) {
    this.memoryLimit = numMegabytes << 20;
    return this;
  }

  public CombinerDefinitionBuilder<T> setKeySizeEstimator(MemoryUsageEstimator<Tuple> keySizeEstimator) {
    this.keySizeEstimator = keySizeEstimator;
    return this;
  }

  public CombinerDefinitionBuilder<T> setValueSizeEstimator(MemoryUsageEstimator<T> valueSizeEstimator) {
    this.valueSizeEstimator = valueSizeEstimator;
    return this;
  }

  public CombinerDefinitionBuilder<T> setStrict(boolean strict) {
    this.strict = strict;
    return this;
  }

  public CombinerDefinitionBuilder<T> setEvictor(Evictor<T> evictor) {
    this.evictor = evictor;
    return this;
  }

  public CombinerDefinition<T> get() {
    if (intermediateFields == null) {
      intermediateFields = outputFields;
    }
    return new CombinerDefinition<T>(
        partialAggregator,
        finalAggregator,
        groupFields,
        inputFields,
        intermediateFields,
        outputFields,
        name,
        limit,
        memoryLimit,
        keySizeEstimator,
        valueSizeEstimator,
        strict,
        evictor,
        keepNullGroups);
  }

  public CombinerDefinitionBuilder setKeepNullGroups(boolean keepNullGroups) {
    this.keepNullGroups = keepNullGroups;
    return this;
  }

  private static class DontEvict<T> implements Evictor<T> {

    @Override
    public boolean shouldEvict(T aggregate) {
      return false;
    }
  }
}
