package com.liveramp.cascading_ext.combiner;

import java.io.Serializable;
import java.util.ArrayList;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.liveramp.commons.util.MemoryUsageEstimator;

public class MultiExactAggregator implements ExactAggregator<ArrayList<Object>> {

  private ExactAggregatorDefinition[] exactAggregatorDefinitions;
  private TupleEntry[] inputTupleEntries;
  private TupleEntry[] intermediateTupleEntries;
  private ArrayList<Integer>[] inputFieldsPos;
  private ArrayList<Integer>[] intermediateFieldsPos;

  public MultiExactAggregator(ExactAggregatorDefinition... exactAggregatorDefinitions) {
    this.exactAggregatorDefinitions = exactAggregatorDefinitions;
  }

  private void prepare() {
    if (inputTupleEntries == null) {
      this.inputTupleEntries = new TupleEntry[exactAggregatorDefinitions.length];
      this.intermediateTupleEntries = new TupleEntry[exactAggregatorDefinitions.length];
      this.inputFieldsPos = new ArrayList[exactAggregatorDefinitions.length];
      this.intermediateFieldsPos = new ArrayList[exactAggregatorDefinitions.length];
      for (int i = 0; i < exactAggregatorDefinitions.length; ++i) {
        inputTupleEntries[i] = new TupleEntry(exactAggregatorDefinitions[i].getInputFields(), Tuple.size(exactAggregatorDefinitions[i].getInputFields().size()));
        intermediateTupleEntries[i] = new TupleEntry(exactAggregatorDefinitions[i].getIntermediateFields(), Tuple.size(exactAggregatorDefinitions[i].getIntermediateFields().size()));
        inputFieldsPos[i] = new ArrayList<Integer>(exactAggregatorDefinitions[i].getInputFields().size());
        intermediateFieldsPos[i] = new ArrayList<Integer>(exactAggregatorDefinitions[i].getIntermediateFields().size());
      }
    }
  }

  @Override
  public ArrayList<Object> initialize() {
    ArrayList<Object> aggregate = new ArrayList<Object>(exactAggregatorDefinitions.length);
    for (int i = 0; i < exactAggregatorDefinitions.length; ++i) {
      aggregate.add(exactAggregatorDefinitions[i].getAggregator().initialize());
    }
    return aggregate;
  }

  @Override
  public ArrayList<Object> partialAggregate(ArrayList<Object> aggregate, TupleEntry nextValue) {
    prepare();
    for (int i = 0; i < exactAggregatorDefinitions.length; ++i) {
      // Load tuple entry
      CombinerUtils.setTupleEntry(inputTupleEntries[i], inputFieldsPos[i], exactAggregatorDefinitions[i].getInputFields(), nextValue);
      // Aggregate
      aggregate.set(i, exactAggregatorDefinitions[i].getAggregator().partialAggregate(aggregate.get(i), inputTupleEntries[i]));
    }
    return aggregate;
  }

  @Override
  public Tuple toPartialTuple(ArrayList<Object> aggregate) {
    Tuple tuple = new Tuple();
    for (int i = 0; i < exactAggregatorDefinitions.length; ++i) {
      tuple.addAll(exactAggregatorDefinitions[i].getAggregator().toPartialTuple(aggregate.get(i)));
    }
    return tuple;
  }

  @Override
  public ArrayList<Object> finalAggregate(ArrayList<Object> aggregate, TupleEntry partialAggregate) {
    prepare();
    for (int i = 0; i < exactAggregatorDefinitions.length; ++i) {
      // Load tuple entry
      CombinerUtils.setTupleEntry(intermediateTupleEntries[i], intermediateFieldsPos[i], exactAggregatorDefinitions[i].getIntermediateFields(), partialAggregate);
      // Aggregate
      aggregate.set(i, exactAggregatorDefinitions[i].getAggregator().finalAggregate(aggregate.get(i), intermediateTupleEntries[i]));
    }
    return aggregate;
  }

  @Override
  public Tuple toFinalTuple(ArrayList<Object> aggregate) {
    Tuple tuple = new Tuple();
    for (int i = 0; i < exactAggregatorDefinitions.length; ++i) {
      tuple.addAll(exactAggregatorDefinitions[i].getAggregator().toFinalTuple(aggregate.get(i)));
    }
    return tuple;
  }
  
  
  public class ObjectValueMemoryUsageEstimator implements MemoryUsageEstimator<ArrayList<Object>>, Serializable {
    
    @Override
    public long estimateMemorySize(ArrayList<Object> items) {
      long size = 12; // ArrayList overhead
      for (int i = 0; i < exactAggregatorDefinitions.length; ++i) {
        if (exactAggregatorDefinitions[i].getValueSizeEstimator() == null) {
          throw new RuntimeException("Memory bound cache used for combining but memory estimator not specified for individual aggregators.");
        }
        
        size += exactAggregatorDefinitions[i].getValueSizeEstimator().estimateMemorySize(items.get(i));
      }
      return size;
    }
    
  }
}
