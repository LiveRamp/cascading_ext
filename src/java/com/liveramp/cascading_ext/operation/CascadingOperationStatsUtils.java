package com.liveramp.cascading_ext.operation;

import cascading.operation.AggregatorCall;
import cascading.operation.ConcreteCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import java.io.IOException;

public class CascadingOperationStatsUtils {

  // The counter category used by the Operation stats decorators
  static public final String COUNTER_CATEGORY = "Stats";

  // Copies a given ConcreteCall and sets the outputCollector.
  public static <Context> ConcreteCall<Context> copyConcreteCallAndSetOutputCollector(AggregatorCall<Context> call, TupleEntryCollector outputCollector) {
    ConcreteCall<Context> newCall = new ConcreteCall<Context>();
    newCall.setArguments(call.getArguments());
    newCall.setContext(call.getContext());
    newCall.setGroup(call.getGroup());
    newCall.setOutputCollector(outputCollector);
    return newCall;
  }

  // A TupleEntryCollectorCounter is a TupleEntryCollector that maintains a
  // count of the number of tuples added to it and forwards them to an
  // underlying collector.
  public static final class TupleEntryCollectorCounter extends TupleEntryCollector {

    private int count = 0;
    private final TupleEntryCollector tupleEntryCollector;

    public TupleEntryCollectorCounter(TupleEntryCollector tupleEntryCollector) {
      this.tupleEntryCollector = tupleEntryCollector;
    }

    public int getCount() {
      return count;
    }

    @Override
    protected void collect(TupleEntry tupleEntry) throws IOException {
      ++count;
      tupleEntryCollector.add(tupleEntry.getTuple());
    }
  }
}
