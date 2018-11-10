package com.liveramp.cascading_ext.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.liveramp.cascading_ext.CascadingUtil;
import com.rapleaf.cascading_ext.datastore.TupleDataStore;

public class DataStoreUtils {

  public static List<Tuple> getAllTuples(Tap tap, CascadingUtil util) throws IOException {

    TupleEntryIterator it = tap.openForRead(util.getFlowProcess());
    List<Tuple> ret = new ArrayList<Tuple>();

    while (it.hasNext()) {
      ret.add(new Tuple(it.next().getTuple())); // need to copy it since TapIterator reuses the same tuple object
    }

    return ret;
  }

  public static void writeToStore(CascadingUtil util, TupleDataStore store, Collection<Tuple> tuples) throws IOException{
    writeToStore(util, store, tuples.toArray(new Tuple[tuples.size()]));
  }

  public static void writeToStore(CascadingUtil util, TupleDataStore store, Tuple ... tuples) throws IOException{
    TupleEntryCollector output = store.getTap().openForWrite(util.getFlowProcess());
    for(Tuple t: tuples){
      output.add(t);
    }
    output.close();
  }

}
