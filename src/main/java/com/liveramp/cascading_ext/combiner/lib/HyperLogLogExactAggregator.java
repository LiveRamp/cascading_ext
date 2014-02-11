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

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.liveramp.cascading_ext.Bytes;
import com.liveramp.cascading_ext.combiner.ExactAggregator;
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;

public class HyperLogLogExactAggregator implements ExactAggregator<ICardinality> {
  
  public static enum OutputOption {CARDINALITY_AND_HLL_OBJECT, CARDINALITY, HLL_OBJECT};
  public static final int DEFAULT_PRECISION = 16;
  public static final OutputOption DEFAULT_OUTPUT_OPTION = OutputOption.CARDINALITY;
  
  private final int precision;
  private OutputOption outputOption;
  
  public HyperLogLogExactAggregator() {
    this(DEFAULT_PRECISION, DEFAULT_OUTPUT_OPTION);
  }
  
  public HyperLogLogExactAggregator(int precision) {
    this(precision, DEFAULT_OUTPUT_OPTION);
  }
  
  public HyperLogLogExactAggregator(OutputOption outputOption) {
    this(DEFAULT_PRECISION, outputOption);
  }
  
  public HyperLogLogExactAggregator(int precision, OutputOption outputOption) {
    this.precision = precision;
    this.outputOption = outputOption;
  }

  @Override
  public ICardinality initialize() {
    return new HyperLogLogPlus(precision);
  }

  @Override
  public ICardinality partialAggregate(ICardinality aggregate, TupleEntry nextValue) {
    aggregate.offer(nextValue.getTuple());
    return aggregate;
  }

  @Override
  public Tuple toPartialTuple(ICardinality aggregate) {
    try {
      return new Tuple(new BytesWritable(aggregate.getBytes()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ICardinality finalAggregate(ICardinality aggregate, TupleEntry partialAggregate) {
    try {
      ICardinality hll = HyperLogLogPlus.Builder.build(Bytes.getBytes((BytesWritable) partialAggregate.getObject(0)));
      aggregate.merge(hll);
      return aggregate;
    } catch (CardinalityMergeException cme) {
      throw new RuntimeException(cme);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Tuple toFinalTuple(ICardinality aggregate) {
    try {
      // Choose what tuple to return according to the outputOption
      switch (outputOption) {
      case CARDINALITY_AND_HLL_OBJECT:
        return new Tuple(new BytesWritable(aggregate.getBytes()), aggregate.cardinality());
        
      case HLL_OBJECT:
        return new Tuple(new BytesWritable(aggregate.getBytes()));
        
      case CARDINALITY:
        return new Tuple(aggregate.cardinality());
        
      default:
        throw new IllegalArgumentException("Invalid value for OutputOption: " + outputOption.toString());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
