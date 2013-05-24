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

package com.liveramp.cascading_ext.tap;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import com.liveramp.cascading_ext.CascadingUtil;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TapHelper {

  public static void writeToTap(Tap<JobConf, ?, ?> t, Tuple... tuples) throws IOException {
    writeToTap(t, CascadingUtil.get().getFlowProcess(), tuples);
  }

  public static void writeToTap(Tap<JobConf, ?, ?> t, FlowProcess<JobConf> conf, Tuple... tuples) throws IOException {
    TupleEntryCollector collector = t.openForWrite(conf);
    for (Tuple tuple : tuples) {
      collector.add(tuple);
    }
    collector.close();
  }

  public static List<TupleEntry> getAllTupleEntries(Tap<JobConf, ?, ?> t) throws IOException {
    TupleEntryIterator iter = t.openForRead(CascadingUtil.get().getFlowProcess());
    List<TupleEntry> tuples = new ArrayList<TupleEntry>();
    while (iter.hasNext()) {
      tuples.add(iter.next());
    }
    return tuples;
  }

  public static List<Tuple> getAllTuples(Tap<JobConf, ?, ?> t) throws IOException {
    return getAllTuples(t, CascadingUtil.get().getFlowProcess());
  }

  public static List<Tuple> getAllTuples(Tap<JobConf, ?, ?> t, FlowProcess<JobConf> conf) throws IOException {
    TupleEntryIterator iter = t.openForRead(conf);
    List<Tuple> tuples = new ArrayList<Tuple>();
    while (iter.hasNext()) {
      tuples.add(iter.next().getTupleCopy());
    }
    return tuples;
  }
}
