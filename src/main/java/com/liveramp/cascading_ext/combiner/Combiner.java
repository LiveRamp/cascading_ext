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

import cascading.pipe.*;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.liveramp.commons.collections.MemoryBoundLruHashMap;
import com.liveramp.commons.util.MemoryUsageEstimator;

public class Combiner<T> extends SubAssembly {

  public static final String COUNTER_GROUP_NAME = "Combiner";
  public static final String INPUT_TUPLES_COUNTER_NAME = "Input tuples";
  public static final String EVICTED_TUPLES_COUNTER_NAME = "Evicted tuples";
  public static final String OUTPUT_TUPLES_COUNTER_NAME = "Output tuples";
  public static final int DEFAULT_LIMIT = 100000;
  public static final boolean DEFAULT_STRICTNESS = false;

  public Combiner(Pipe[] pipes,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields secondarySortFields,
                  Fields inputFields,
                  Fields intermediateFields,
                  Fields outputFields,
                  int itemLimit,
                  long memoryLimit,
                  MemoryUsageEstimator<Tuple> keySizeEstimator,
                  MemoryUsageEstimator<T> valueSizeEstimator,
                  boolean strict) {
    super(pipes);
    Pipe[] pipesCopy = new Pipe[pipes.length];
    for (int i = 0; i < pipes.length; i++) {
      pipesCopy[i] = new Each(
          pipes[i],
          Combiner.function(aggregator, groupFields, inputFields, intermediateFields, itemLimit, memoryLimit, keySizeEstimator, valueSizeEstimator, strict),
          Fields.RESULTS);
    }
    Pipe pipe = new GroupBy(pipesCopy, groupFields, secondarySortFields);
    pipe = new Every(pipe, Combiner.aggregator(aggregator, groupFields, intermediateFields, outputFields), Fields.RESULTS);
    setTails(pipe);
  }

  public Combiner(Pipe[] pipes, ExactAggregator<T> aggregator, Fields secondarySortFields, CombinerDefinition<T> combinerDefinition) {
    this(pipes, aggregator, combinerDefinition.getGroupFields(), secondarySortFields, combinerDefinition.getInputFields(),
        combinerDefinition.getIntermediateFields(), combinerDefinition.getOutputFields(), combinerDefinition.getLimit(),
        combinerDefinition.getMemoryLimit(), combinerDefinition.getKeySizeEstimator(),
        combinerDefinition.getValueSizeEstimator(), combinerDefinition.isStrict());
  }

  public Combiner(Pipe[] pipes, ExactAggregator<T> aggregator, CombinerDefinition<T> combinerDefinition) {
    this(pipes, aggregator, null, combinerDefinition);
  }

  public Combiner(Pipe[] pipes,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields inputFields,
                  Fields intermediateFields,
                  Fields outputFields,
                  int limit,
                  boolean strict) {
    this(pipes,
        aggregator,
        groupFields,
        null,
        inputFields,
        intermediateFields,
        outputFields,
        limit,
        MemoryBoundLruHashMap.UNLIMITED_MEMORY_CAPACITY,
        null,
        null,
        strict);
  }

  public Combiner(Pipe pipe,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields inputFields,
                  Fields outputFields) {
    this(pipe,
        aggregator,
        groupFields,
        inputFields,
        outputFields,
        DEFAULT_LIMIT,
        DEFAULT_STRICTNESS);
  }

  public Combiner(Pipe pipe,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields inputFields,
                  Fields intermediateFields,
                  Fields outputFields) {
    this(pipe,
        aggregator,
        groupFields,
        inputFields,
        intermediateFields,
        outputFields,
        DEFAULT_LIMIT,
        DEFAULT_STRICTNESS);
  }

  public Combiner(Pipe pipe,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields inputFields,
                  Fields outputFields,
                  int limit) {
    this(pipe, aggregator, groupFields, inputFields, outputFields, limit, DEFAULT_STRICTNESS);
  }

  public Combiner(Pipe pipe,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields inputFields,
                  Fields intermediateFields,
                  Fields outputFields,
                  int limit) {
    this(pipe, aggregator, groupFields, inputFields, intermediateFields, outputFields, limit, DEFAULT_STRICTNESS);
  }

  public Combiner(Pipe pipe,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields inputFields,
                  Fields outputFields,
                  boolean strict) {
    this(pipe, aggregator, groupFields, inputFields, outputFields, DEFAULT_LIMIT, strict);
  }

  public Combiner(Pipe pipe,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields inputFields,
                  Fields intermediateFields,
                  Fields outputFields,
                  boolean strict) {
    this(pipe, aggregator, groupFields, inputFields, intermediateFields, outputFields, DEFAULT_LIMIT, strict);
  }

  public Combiner(Pipe pipe,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields inputFields,
                  Fields outputFields,
                  int limit,
                  boolean strict) {
    this(new Pipe[]{pipe}, aggregator, groupFields, inputFields, outputFields, outputFields, limit, strict);
  }

  public Combiner(Pipe pipe,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields inputFields,
                  Fields intermediateFields,
                  Fields outputFields,
                  int limit,
                  boolean strict) {
    this(new Pipe[]{pipe}, aggregator, groupFields, inputFields, intermediateFields, outputFields, limit, strict);
  }

  public Combiner(Pipe[] pipes,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields inputFields,
                  Fields outputFields) {
    this(pipes,
        aggregator,
        groupFields,
        inputFields,
        outputFields,
        outputFields,
        DEFAULT_LIMIT,
        DEFAULT_STRICTNESS);
  }

  public Combiner(Pipe[] pipes,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields inputFields,
                  Fields intermediateFields,
                  Fields outputFields) {
    this(pipes,
        aggregator,
        groupFields,
        inputFields,
        intermediateFields,
        outputFields,
        DEFAULT_LIMIT,
        DEFAULT_STRICTNESS);
  }

  public Combiner(Pipe[] pipes,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields inputFields,
                  Fields outputFields,
                  int limit) {
    this(pipes, aggregator, groupFields, inputFields, outputFields, outputFields, limit, DEFAULT_STRICTNESS);
  }

  public Combiner(Pipe[] pipes,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields inputFields,
                  Fields intermediateFields,
                  Fields outputFields,
                  int limit) {
    this(pipes, aggregator, groupFields, inputFields, intermediateFields, outputFields, limit, DEFAULT_STRICTNESS);
  }

  public Combiner(Pipe[] pipes,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields inputFields,
                  Fields outputFields,
                  boolean strict) {
    this(pipes, aggregator, groupFields, inputFields, outputFields, outputFields, DEFAULT_LIMIT, strict);
  }

  public Combiner(Pipe[] pipes,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields inputFields,
                  Fields intermediateFields,
                  Fields outputFields,
                  boolean strict) {
    this(pipes, aggregator, groupFields, inputFields, intermediateFields, outputFields, DEFAULT_LIMIT, strict);
  }

  public Combiner(Pipe[] pipes,
                  ExactAggregator<T> aggregator,
                  Fields groupFields,
                  Fields inputFields,
                  Fields outputFields,
                  int limit,
                  boolean strict) {
    this(pipes, aggregator, groupFields, inputFields, outputFields, outputFields, limit, strict);
  }

  public static <T> Combiner<T> assembly(Pipe pipe,
                                         ExactAggregator<T> aggregator,
                                         Fields groupFields,
                                         Fields inputFields,
                                         Fields outputFields) {
    return new Combiner<T>(pipe, aggregator, groupFields, inputFields, outputFields);
  }

  public static <T> Combiner<T> assembly(Pipe pipe,
                                         ExactAggregator<T> aggregator,
                                         Fields groupFields,
                                         Fields inputFields,
                                         Fields intermediateFields,
                                         Fields outputFields) {
    return new Combiner<T>(pipe, aggregator, groupFields, inputFields, intermediateFields, outputFields);
  }

  public static <T> Combiner<T> assembly(Pipe pipe,
                                         ExactAggregator<T> aggregator,
                                         Fields groupFields,
                                         Fields inputFields,
                                         Fields outputFields,
                                         int limit) {
    return new Combiner<T>(pipe, aggregator, groupFields, inputFields, outputFields, limit);
  }

  public static <T> Combiner<T> assembly(Pipe pipe,
                                         ExactAggregator<T> aggregator,
                                         Fields groupFields,
                                         Fields inputFields,
                                         Fields intermediateFields,
                                         Fields outputFields,
                                         int limit) {
    return new Combiner<T>(pipe, aggregator, groupFields, inputFields, intermediateFields, outputFields, limit);
  }

  public static <T> Combiner<T> assembly(Pipe pipe,
                                         ExactAggregator<T> aggregator,
                                         Fields groupFields,
                                         Fields inputFields,
                                         Fields outputFields,
                                         boolean strict) {
    return new Combiner<T>(pipe, aggregator, groupFields, inputFields, outputFields, strict);
  }

  public static <T> Combiner<T> assembly(Pipe pipe,
                                         ExactAggregator<T> aggregator,
                                         Fields groupFields,
                                         Fields inputFields,
                                         Fields intermediateFields,
                                         Fields outputFields,
                                         boolean strict) {
    return new Combiner<T>(pipe, aggregator, groupFields, inputFields, intermediateFields, outputFields, strict);
  }

  public static <T> Combiner<T> assembly(Pipe pipe,
                                         ExactAggregator<T> aggregator,
                                         Fields groupFields,
                                         Fields inputFields,
                                         Fields outputFields,
                                         int limit,
                                         boolean strict) {
    return new Combiner<T>(pipe, aggregator, groupFields, inputFields, outputFields, limit, strict);
  }

  public static <T> Combiner<T> assembly(Pipe pipe,
                                         ExactAggregator<T> aggregator,
                                         Fields groupFields,
                                         Fields inputFields,
                                         Fields intermediateFields,
                                         Fields outputFields,
                                         int limit,
                                         boolean strict) {
    return new Combiner<T>(pipe, aggregator, groupFields, inputFields, intermediateFields, outputFields, limit, strict);
  }

  public static <T> Combiner<T> assembly(Pipe pipe,
                                         ExactAggregator<T> aggregator,
                                         CombinerDefinition<T> combinerDefinition) {
    return new Combiner<T>(new Pipe[]{pipe}, aggregator, combinerDefinition);
  }

  public static <T> Combiner<T> assembly(Pipe[] pipes,
                                         ExactAggregator<T> aggregator,
                                         Fields groupFields,
                                         Fields inputFields,
                                         Fields outputFields) {
    return new Combiner<T>(pipes, aggregator, groupFields, inputFields, outputFields);
  }

  public static <T> Combiner<T> assembly(Pipe[] pipes,
                                         ExactAggregator<T> aggregator,
                                         Fields groupFields,
                                         Fields inputFields,
                                         Fields intermediateFields,
                                         Fields outputFields) {
    return new Combiner<T>(pipes, aggregator, groupFields, inputFields, intermediateFields, outputFields);
  }

  public static <T> Combiner<T> assembly(Pipe[] pipes,
                                         ExactAggregator<T> aggregator,
                                         Fields groupFields,
                                         Fields inputFields,
                                         Fields outputFields,
                                         int limit) {
    return new Combiner<T>(pipes, aggregator, groupFields, inputFields, outputFields, limit);
  }

  public static <T> Combiner<T> assembly(Pipe[] pipes,
                                         ExactAggregator<T> aggregator,
                                         Fields groupFields,
                                         Fields inputFields,
                                         Fields intermediateFields,
                                         Fields outputFields,
                                         int limit) {
    return new Combiner<T>(pipes, aggregator, groupFields, inputFields, intermediateFields, outputFields, limit);
  }

  public static <T> Combiner<T> assembly(Pipe[] pipes,
                                         ExactAggregator<T> aggregator,
                                         Fields groupFields,
                                         Fields inputFields,
                                         Fields outputFields,
                                         boolean strict) {
    return new Combiner<T>(pipes, aggregator, groupFields, inputFields, outputFields, strict);
  }

  public static <T> Combiner<T> assembly(Pipe[] pipes,
                                         ExactAggregator<T> aggregator,
                                         Fields groupFields,
                                         Fields inputFields,
                                         Fields intermediateFields,
                                         Fields outputFields,
                                         boolean strict) {
    return new Combiner<T>(pipes, aggregator, groupFields, inputFields, intermediateFields, outputFields, strict);
  }

  public static <T> Combiner<T> assembly(Pipe[] pipes,
                                         ExactAggregator<T> aggregator,
                                         Fields groupFields,
                                         Fields inputFields,
                                         Fields outputFields,
                                         int limit,
                                         boolean strict) {
    return new Combiner<T>(pipes, aggregator, groupFields, inputFields, outputFields, outputFields, limit, strict);
  }

  public static <T> Combiner<T> assembly(Pipe[] pipes,
                                         ExactAggregator<T> aggregator,
                                         Fields groupFields,
                                         Fields inputFields,
                                         Fields intermediateFields,
                                         Fields outputFields,
                                         int limit,
                                         boolean strict) {
    return new Combiner<T>(pipes, aggregator, groupFields, inputFields, intermediateFields, outputFields, limit, strict);
  }


  public static <T> CombinerFunction<T> function(PartialAggregator<T> aggregator,
                                                 Fields groupFields,
                                                 Fields inputFields,
                                                 Fields outputFields) {
    return function(
        aggregator,
        groupFields,
        inputFields,
        outputFields,
        DEFAULT_LIMIT,
        MemoryBoundLruHashMap.UNLIMITED_MEMORY_CAPACITY,
        null,
        null,
        DEFAULT_STRICTNESS);
  }

  public static <T> CombinerFunction<T> function(PartialAggregator<T> aggregator,
                                                 Fields groupFields,
                                                 Fields inputFields,
                                                 Fields outputFields,
                                                 int limit) {
    return function(aggregator, groupFields, inputFields, outputFields, limit, MemoryBoundLruHashMap.UNLIMITED_MEMORY_CAPACITY, null, null, DEFAULT_STRICTNESS);
  }

  public static <T> CombinerFunction<T> function(PartialAggregator<T> aggregator,
                                                 Fields groupFields,
                                                 Fields inputFields,
                                                 Fields outputFields,
                                                 boolean strict) {
    return function(aggregator, groupFields, inputFields, outputFields, DEFAULT_LIMIT, MemoryBoundLruHashMap.UNLIMITED_MEMORY_CAPACITY, null, null, strict);
  }

  public static <T> CombinerFunction<T> function(PartialAggregator<T> aggregator,
                                                 Fields groupFields,
                                                 Fields inputFields,
                                                 Fields outputFields,
                                                 int itemLimit,
                                                 long memoryLimit,
                                                 MemoryUsageEstimator<Tuple> keySizeEstimator,
                                                 MemoryUsageEstimator<T> valueSizeEstimator,
                                                 boolean strict) {
    return new CombinerFunction<T>(aggregator, groupFields, inputFields, outputFields, itemLimit, memoryLimit, keySizeEstimator, valueSizeEstimator, strict);
  }

  public static <T> CombinerAggregator<T> aggregator(FinalAggregator<T> aggregator,
                                                     Fields groupFields,
                                                     Fields inputFields,
                                                     Fields outputFields) {
    return new CombinerAggregator<T>(aggregator, groupFields, inputFields, outputFields);
  }
}
