package com.liveramp.cascading_ext.operation;

import cascading.operation.*;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import java.io.IOException;
import java.io.Serializable;

public class OperationStatsUtils {
  // The counter category used by the Operation stats decorators
  static public final String COUNTER_CATEGORY = "Stats";

  public static final class TupleEntryCollectorCounter extends TupleEntryCollector implements Serializable{

    private int count = 0;
    private TupleEntryCollector tupleEntryCollector;

    public void setOutputCollector(TupleEntryCollector collector){
      count = 0;
      this.tupleEntryCollector = collector;
    }

    public int getCount() {
      return count;
    }

    @Override
    protected void collect(TupleEntry tupleEntry) throws IOException {
      tupleEntryCollector.add(tupleEntry.getTuple());
    }

    public void setFields(cascading.tuple.Fields declared) {
      tupleEntryCollector.setFields(declared);
    }

    public void add(cascading.tuple.TupleEntry tupleEntry) {
      count++;
      tupleEntryCollector.add(tupleEntry);
    }

    public void add(cascading.tuple.Tuple tuple) {
      count++;
      tupleEntryCollector.add(tuple);
    }

    public void close() {
      tupleEntryCollector.close();
    }
  }

  public static class ForwardingOperationCall<Context, Call extends OperationCall<Context>> implements OperationCall<Context>, Serializable {
    protected Call delegate;
    protected TupleEntryCollectorCounter collector = new TupleEntryCollectorCounter();

    public void setDelegate(Call delegate){
      this.delegate = delegate;
    }

    @Override
    public Context getContext() {
      return delegate.getContext();
    }

    @Override
    public void setContext(Context context) {
      delegate.setContext(context);
    }

    @Override
    public Fields getArgumentFields() {
      return delegate.getArgumentFields();
    }

    public TupleEntryCollectorCounter getOutputCollector() {
      return collector;
    }
  }
}
