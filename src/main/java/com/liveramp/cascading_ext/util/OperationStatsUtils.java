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

package com.liveramp.cascading_ext.util;

import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import java.io.IOException;
import java.io.Serializable;

public class OperationStatsUtils {

  // The counter category used by the Operation stats decorators
  static public final String DEFAULT_COUNTER_CATEGORY = "Stats";

  public static final class TupleEntryCollectorCounter extends TupleEntryCollector implements Serializable {

    private int count = 0;
    private TupleEntryCollector tupleEntryCollector;

    public void setOutputCollector(TupleEntryCollector collector) {
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

    @Override
    public void setFields(cascading.tuple.Fields declared) {
      tupleEntryCollector.setFields(declared);
    }

    @Override
    public void add(cascading.tuple.TupleEntry tupleEntry) {
      count++;
      tupleEntryCollector.add(tupleEntry);
    }

    @Override
    public void add(cascading.tuple.Tuple tuple) {
      count++;
      tupleEntryCollector.add(tuple);
    }

    @Override
    public void close() {
      tupleEntryCollector.close();
    }
  }

  public static abstract class ForwardingOperationCall<Context, Call extends OperationCall<Context>> implements OperationCall<Context>, Serializable {
    protected Call delegate;
    protected TupleEntryCollectorCounter collector = new TupleEntryCollectorCounter();

    public void setDelegate(Call delegate) {
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

    public abstract Fields getDeclaredFields();

    public TupleEntryCollectorCounter getOutputCollector() {
      return collector;
    }
  }

  public static StackTraceElement getStackPosition(int depth) {
    StackTraceElement[] stackTrace = new Throwable().getStackTrace();
    return stackTrace[depth + 1];
  }

  public static String formatStackPosition(StackTraceElement stackTraceElement) {
    return stackTraceElement.getFileName() + ":"
        + stackTraceElement.getMethodName() + ":"
        + stackTraceElement.getLineNumber();
  }
}
