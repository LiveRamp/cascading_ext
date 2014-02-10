package com.liveramp.cascading_ext.combiner;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import java.io.Serializable;

public class MultiCombinerTupleEmitter implements Serializable {
  private transient TupleEntry resuableEntry;
  private transient TupleEntryCollector collector;
  private Fields allFields;

  public MultiCombinerTupleEmitter(Fields allFields) {
    this.allFields = allFields;
  }

  public void setCollector(TupleEntryCollector collector) {
    this.collector = collector;
  }

  public TupleEntry allocateTuple(Fields subSetOfFields, Object... values) {
    if (resuableEntry == null) {
      resuableEntry = new TupleEntry(allFields, Tuple.size(allFields.size()));
    }
    for (Comparable field : resuableEntry.getFields()) {
      resuableEntry.setObject(field, null);
    }
    for (int i = 0; i < subSetOfFields.size(); i++) {
      resuableEntry.setObject(subSetOfFields.get(i), values[i]);
    }
    return resuableEntry;
  }

  public void emit(Fields subSetOfFields, Object... values) {
    collector.add(allocateTuple(subSetOfFields, values));
  }
}
