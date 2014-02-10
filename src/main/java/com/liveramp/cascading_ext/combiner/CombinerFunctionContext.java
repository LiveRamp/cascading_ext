package com.liveramp.cascading_ext.combiner;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.collect.Lists;
import com.liveramp.commons.collections.MemoryBoundLruHashMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CombinerFunctionContext<T> implements Serializable {

  private final CombinerDefinition<T> definition;
  private final String counterGroupName;

  private MemoryBoundLruHashMap<Tuple, T> cache;
  private TupleEntry key;
  private TupleEntry input;
  private ArrayList<Integer> keyFieldsPos;
  private ArrayList<Integer> inputFieldsPos;

  public CombinerFunctionContext(CombinerDefinition<T> definition) {
    this.definition = definition;
    this.counterGroupName = Combiner.COUNTER_GROUP_NAME + "-" + definition.getName();
  }

  public void prepare() {
    cache = new MemoryBoundLruHashMap<Tuple, T>(definition.getLimit(),
        definition.getMemoryLimit(),
        definition.getKeySizeEstimator(),
        definition.getValueSizeEstimator());
    key = new TupleEntry(definition.getGroupFields(), Tuple.size(definition.getGroupFields().size()));
    input = new TupleEntry(definition.getInputFields(), Tuple.size(definition.getInputFields().size()));
    keyFieldsPos = Lists.newArrayList();
    inputFieldsPos = Lists.newArrayList();
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

  public List<Tuple> combineAndEvict(FlowProcess flow) {
    if (containsNonNullField(key) || definition.shouldKeepNullGroups()) {
      long bytesBefore = 0;
      if (cache.isMemoryBound()) {
        bytesBefore = cache.getNumManagedBytes();
      }

      List<Tuple> evictedTuples = null;
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
        } else {
          newAggregate = aggregator.partialAggregate(aggregate, input);
        }

        if (definition.getEvictor().shouldEvict(newAggregate)) {
          cache.remove(key.getTuple());
          evictedTuples = new LinkedList<Tuple>();
          evictedTuples.add(evictTuple(key.getTuple(), newAggregate, cache));
        } else if (newAggregate != aggregate) {
          // If aggregate was not updated in place by update(), put the new object in the map
          // Note: an entry for this key already exists in the map, so we do not need to
          // deep copy the key when putting (overwriting) the new aggregate value.
          List<Map.Entry<Tuple, T>> evicted = cache.putAndEvict(key.getTuple(), newAggregate);
          // The new aggregate could be bigger than the previous one so some items may have been evicted.
          if (!evicted.isEmpty()) {
            evictedTuples = new LinkedList<Tuple>();
            for (Map.Entry<Tuple, T> entry : evicted) {
              evictedTuples.add(evictTuple(entry.getKey(), entry.getValue(), cache));
            }
          }
        }
      } else {
        // If we don't have the key in our map then we add the value for the key
        T newAggregate = aggregator.partialAggregate(aggregator.initialize(), input);
        if (newAggregate == null) {
          throw new RuntimeException(this.getClass().getSimpleName() + " is not designed to work with aggregate values that are null.");
        }
        List<Map.Entry<Tuple, T>> evicted = cache.putAndEvict(definition.getTupleCopier().deepCopy(key.getTuple()), newAggregate);
        if (flow != null) {
          flow.increment("Combiner", "Num Items", 1);
        }
        // While adding the new key we may have caused one to get evicted. If any have been evicted
        // then we must make the evicted key/values into tuples and emit them.
        if (!evicted.isEmpty()) {
          evictedTuples = new LinkedList<Tuple>();
          for (Map.Entry<Tuple, T> entry : evicted) {
            evictedTuples.add(evictTuple(entry.getKey(), entry.getValue(), cache));
          }
        }
      }

      if (flow != null) {
        if (evictedTuples != null) {
          flow.increment("Combiner", "Num Items", -1 * evictedTuples.size());
        }

        if (cache.isMemoryBound()) {
          long sizeChange = cache.getNumManagedBytes() - bytesBefore;
          if (sizeChange != 0) {
            flow.increment("Combiner", "Num Bytes", sizeChange);
          }
        }
      }

      return evictedTuples;
    }
    return null;
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
      throw new IllegalStateException("A strict " + this.getClass().getSimpleName() + " should never evict."
          + " key: " + key + ", aggregate: " + aggregate + ", combiner size: " + map.size());
    }
    return createResultTuple(key, aggregate);
  }

  private Tuple createResultTuple(Tuple key, T aggregate) {
    return key.append(definition.getPartialAggregator().toPartialTuple(aggregate));
  }
}
