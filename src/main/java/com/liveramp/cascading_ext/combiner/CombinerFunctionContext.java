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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.hadoop.mapred.JobConf;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.liveramp.cascading_ext.TupleSerializationUtil;
import com.liveramp.commons.collections.MemoryBoundLruHashMap;

public class CombinerFunctionContext<T> implements Serializable {

  public interface OutputHandler {
    void handleOutput(CombinerFunctionContext context, Tuple tuple, boolean evicted);
  }

  private final CombinerDefinition<T> definition;
  private final String counterGroupName;

  private MemoryBoundLruHashMap<Tuple, T> cache;
  private TupleEntry key;
  private TupleEntry input;
  private ArrayList<Integer> keyFieldsPos;
  private ArrayList<Integer> inputFieldsPos;
  private TupleSerializationUtil serializationUtil;

  public CombinerFunctionContext(CombinerDefinition<T> definition) {
    this.definition = definition;
    this.counterGroupName = Combiner.COUNTER_GROUP_NAME + "-" + definition.getName();
  }

  public void prepare(FlowProcess<JobConf> flow) {
    this.prepare(flow.getConfigCopy());
  }

  public void prepare(JobConf conf) {
    cache = new MemoryBoundLruHashMap<Tuple, T>(definition.getLimit(),
        definition.getMemoryLimit(),
        definition.getKeySizeEstimator(),
        definition.getValueSizeEstimator());
    key = new TupleEntry(definition.getGroupFields(), Tuple.size(definition.getGroupFields().size()));
    input = new TupleEntry(definition.getInputFields(), Tuple.size(definition.getInputFields().size()));
    keyFieldsPos = Lists.newArrayList();
    inputFieldsPos = Lists.newArrayList();
    this.serializationUtil = new TupleSerializationUtil(conf);
  }

  public void setGroupFields(FunctionCall call) {
    setGroupFields(call.getArguments());
  }

  public void setGroupFields(TupleEntry arguments) {
    CombinerUtils.setTupleEntry(key, keyFieldsPos, definition.getGroupFields(), arguments);
  }

  public void setInputFields(FunctionCall call) {
    setInputFields(call.getArguments());
  }

  public void setInputFields(TupleEntry arguments) {
    CombinerUtils.setTupleEntry(input, inputFieldsPos, definition.getInputFields(), arguments);
  }

  public void combineAndEvict(FlowProcess flow, OutputHandler outputHandler) {
    if (containsNonNullField(key) || definition.shouldKeepNullGroups()) {
      long bytesBefore = 0;
      if (cache.isMemoryBound()) {
        bytesBefore = cache.getNumManagedBytes();
      }

      PartialAggregator<T> aggregator = definition.getPartialAggregator();
      T aggregate = cache.get(key.getTuple());
      // If we have a value for the groupFields in our map then we update them.
      if (aggregate != null) {
        T newAggregate;

        if (cache.isMemoryBound()) {
          long previousSize = cache.estimateValueMemorySize(aggregate);
          newAggregate = aggregator.partialAggregate(aggregate, input);
          long newSize = cache.estimateValueMemorySize(newAggregate);
          cache.adjustNumManagedBytes(newSize - previousSize);
          if (newSize > previousSize) {
            List<Map.Entry<Tuple, T>> evicted = cache.evictIfNecessary();
            outputEvictedTuples(flow, outputHandler, evicted);
          }
        } else {
          newAggregate = aggregator.partialAggregate(aggregate, input);
        }

        if (definition.getEvictor().shouldEvict(newAggregate)) {
          cache.remove(key.getTuple());
          outputHandler.handleOutput(this, evictTuple(key.getTuple(), newAggregate, cache), true);
          outputEvictedTuplesCounter(flow, 1);
        } else if (newAggregate != aggregate) {
          // If aggregate was not updated in place by update(), put the new object in the map
          // Note: an entry for this key already exists in the map, so we do not need to
          // deep copy the key when putting (overwriting) the new aggregate value.
          List<Map.Entry<Tuple, T>> evicted = cache.putAndEvict(key.getTuple(), newAggregate);
          // The new aggregate could be bigger than the previous one so some items may have been evicted.
          outputEvictedTuples(flow, outputHandler, evicted);
        }
      } else {
        // If we don't have the key in our map then we add the value for the key
        T newAggregate = aggregator.partialAggregate(aggregator.initialize(), input);
        if (newAggregate == null) {
          throw new RuntimeException(this.getClass().getSimpleName() + " is not designed to work with aggregate values that are null.");
        }
        List<Map.Entry<Tuple, T>> evicted = cache.putAndEvict(deepCopy(key.getTuple()), newAggregate);
        if (flow != null) {
          flow.increment("Combiner", "Num Items", 1);
        }
        // While adding the new key we may have caused one to get evicted. If any have been evicted
        // then we must make the evicted key/values into tuples and emit them.
        outputEvictedTuples(flow, outputHandler, evicted);
      }

      if (flow != null) {

        if (cache.isMemoryBound()) {
          long sizeChange = cache.getNumManagedBytes() - bytesBefore;
          if (sizeChange != 0) {
            flow.increment("Combiner", "Num Bytes", sizeChange);
          }
        }
      }
    }
  }

  private void outputEvictedTuples(FlowProcess flow, OutputHandler outputHandler, List<Map.Entry<Tuple, T>> evicted) {
    if (!evicted.isEmpty()) {
      for (Map.Entry<Tuple, T> entry : evicted) {
        outputHandler.handleOutput(this, evictTuple(entry.getKey(), entry.getValue(), cache), true);
      }
      outputEvictedTuplesCounter(flow, evicted.size());
    }
  }

  private void outputEvictedTuplesCounter(FlowProcess flow, int numEvictedTuples) {
    if (flow != null) {
      if (numEvictedTuples > 0) {
        flow.increment("Combiner", "Num Items", -1 * numEvictedTuples);
        if (numEvictedTuples < 10) {
          flow.increment("Combiner", "Num Evicted Tuples <10", 1);
        } else if (numEvictedTuples < 100) {
          flow.increment("Combiner", "Num Evicted Tuples <100", 1);
        } else {
          flow.increment("Combiner", "Num Evicted Tuples >=100", 1);
        }
      }
    }
  }

  private Tuple deepCopy(Tuple tuple) {
    try {
      return serializationUtil.deserialize(serializationUtil.serialize(tuple));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean containsNonNullField(TupleEntry key) {
    for (Object value : key.getTuple()) {
      if (value != null) {
        return true;
      }
    }
    return false;
  }

  public Iterator<Tuple> cacheTuplesIterator() {
    return new ResultTupleIterator(cache.iterator());
  }

  public CombinerDefinition<T> getDefinition() {
    return definition;
  }

  public String getCounterGroupName() {
    return counterGroupName;
  }

  private class ResultTupleIterator implements Iterator<Tuple> {

    Iterator<Map.Entry<Tuple, T>> mapEntryIterator;

    private ResultTupleIterator(Iterator<Map.Entry<Tuple, T>> mapEntryIterator) {
      this.mapEntryIterator = mapEntryIterator;
    }

    @Override
    public boolean hasNext() {
      return mapEntryIterator.hasNext();
    }

    @Override
    public Tuple next() {
      Map.Entry<Tuple, T> entry = mapEntryIterator.next();
      return createResultTuple(entry.getKey(), entry.getValue());
    }

    @Override
    public void remove() {
      mapEntryIterator.remove();
    }
  }

  private Tuple evictTuple(Tuple key, T aggregate, MemoryBoundLruHashMap<Tuple, T> map) {
    if (definition.isStrict()) {
      throw new IllegalStateException("A strict " + this.getClass().getSimpleName() + " should never evictIfNecessary."
          + " key: " + key + ", aggregate: " + aggregate + ", combiner size: " + map.size());
    }
    return createResultTuple(key, aggregate);
  }

  private Tuple createResultTuple(Tuple key, T aggregate) {
    return key.append(definition.getPartialAggregator().toPartialTuple(aggregate));
  }
}
