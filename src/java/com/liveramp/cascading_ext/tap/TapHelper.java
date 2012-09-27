package com.liveramp.cascading_ext.tap;

import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import com.liveramp.cascading_ext.CascadingUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TapHelper {

  public static void writeToTap(Tap t, Tuple... tuples) throws IOException {
    TupleEntryCollector collector = t.openForWrite(CascadingUtil.get().getFlowProcess());
    for(Tuple tuple: tuples){
      collector.add(tuple);
    }
    collector.close();
  }

  public static List<TupleEntry> getAllTupleEntries(Tap t) throws IOException {
    TupleEntryIterator iter = t.openForRead(CascadingUtil.get().getFlowProcess());
    List<TupleEntry> tuples = new ArrayList<TupleEntry>();
    while(iter.hasNext()){
      tuples.add(iter.next());
    }
    return tuples;
  }

  public static List<Tuple> getAllTuples(Tap t) throws IOException {
    TupleEntryIterator iter = t.openForRead(CascadingUtil.get().getFlowProcess());
    List<Tuple> tuples = new ArrayList<Tuple>();
    while(iter.hasNext()){
      tuples.add(iter.next().getTupleCopy());
    }
    return tuples;
  }
}
