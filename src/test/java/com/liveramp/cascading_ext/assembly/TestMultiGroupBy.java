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

package com.liveramp.cascading_ext.assembly;

import cascading.operation.filter.FilterNull;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import com.google.common.collect.Lists;
import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.multi_group_by.MultiBuffer;
import com.liveramp.cascading_ext.tap.TapHelper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestMultiGroupBy extends BaseTestCase {

  private Hfs source1;
  private Hfs source2;

  private final String SOURCE1 = getTestRoot() + "/mgb_source1";
  private final String SOURCE2 = getTestRoot() + "/mgb_source2";

  @Before
  public void setUp() throws IOException {

    source1 = new Hfs(new SequenceFile(new Fields("key", "num")), SOURCE1);
    TapHelper.writeToTap(source1,
        new Tuple(1, 1),
        new Tuple(1, 3),
        new Tuple(1, 2),
        new Tuple(2, 5),
        new Tuple(3, 3),
        new Tuple(3, 3));

    source2 = new Hfs(new SequenceFile(new Fields("key", "num1", "num2")), SOURCE2);
    TapHelper.writeToTap(source2,
        new Tuple(1, 101, 1),
        new Tuple(5, 5, 2),
        new Tuple(3, 0, 0));
  }

  @Test
  public void testSimple3() throws Exception {

    final String OUTPUT = getTestRoot() + "/mgb_output";

    Hfs sink = new Hfs(new SequenceFile(new Fields("key", "result", " result1", "result2", "result3", "result4", "result5")), OUTPUT);

    Map<String, Tap> sources = new HashMap<String, Tap>();
    sources.put("s1", source1);
    sources.put("s2", source2);

    Pipe s1 = new Pipe("s1");
    Pipe s2 = new Pipe("s2");

    Pipe results = new MultiGroupBy(new Pipe[]{s1, s2},
        new Fields[]{new Fields("key", "num"), new Fields("key", "num1", "num2")},
        new Fields[]{new Fields("key"), new Fields("key")},
        new Fields("key"),
        new CustomBuffer(new Fields("result", " result1", "result2", "result3", "result4", "result5")));
    results = new Each(results, new Fields("key"), new FilterNull());

    CascadingUtil.get().getFlowConnector().connect(sources, sink, results).complete();

    TupleEntryIterator iter = sink.openForRead(CascadingUtil.get().getFlowProcess());

    assertEquals(new Tuple(1, 108, 108, 108, 108, 108, 108), iter.next().getTuple());
    assertEquals(new Tuple(2, 5, 5, 5, 5, 5, 5), iter.next().getTuple());
    assertEquals(new Tuple(3, 6, 6, 6, 6, 6, 6), iter.next().getTuple());
    assertEquals(new Tuple(5, 7, 7, 7, 7, 7, 7), iter.next().getTuple());

    assertFalse(iter.hasNext());
  }


  @Test
  public void testSimple() throws Exception {
    final String OUTPUT = getTestRoot() + "/mgb_output";

    Hfs sink = new Hfs(new SequenceFile(new Fields("key-rename", "result", " result1", "result2", "result3", "result4", "result5")), OUTPUT);

    Map<String, Tap> sources = new HashMap<String, Tap>();
    sources.put("s1", source1);
    sources.put("s2", source2);

    Pipe s1 = new Pipe("s1");
    s1 = new Retain(s1, new Fields("key", "num"));

    Pipe s2 = new Pipe("s2");
    s2 = new Retain(s2, new Fields("key", "num1", "num2"));

    Pipe results = new MultiGroupBy(s1, new Fields("key"), s2, new Fields("key"),
        new Fields("key-rename"), new CustomBuffer(new Fields("result", " result1", "result2", "result3", "result4", "result5")));

    CascadingUtil.get().getFlowConnector().connect(sources, sink, results).complete();

    TupleEntryIterator iter = sink.openForRead(CascadingUtil.get().getFlowProcess());

    assertEquals(new Tuple(1, 108, 108, 108, 108, 108, 108), iter.next().getTuple());
    assertEquals(new Tuple(2, 5, 5, 5, 5, 5, 5), iter.next().getTuple());
    assertEquals(new Tuple(3, 6, 6, 6, 6, 6, 6), iter.next().getTuple());
    assertEquals(new Tuple(5, 7, 7, 7, 7, 7, 7), iter.next().getTuple());

    assertFalse(iter.hasNext());
  }

  @Test
  public void testSimple2() throws Exception {
    final String OUTPUT = getTestRoot() + "/mgb_output";

    Hfs sink = new Hfs(new SequenceFile(new Fields("key-rename", "result")), OUTPUT);

    Map<String, Tap> sources = new HashMap<String, Tap>();
    sources.put("s1", source1);
    sources.put("s2", source2);

    Pipe s1 = new Pipe("s1");
    s1 = new Retain(s1, new Fields("key", "num"));

    Pipe s2 = new Pipe("s2");
    s2 = new Retain(s2, new Fields("key", "num1", "num2"));

    Pipe results = new MultiGroupBy(s1, new Fields("key"), s2, new Fields("key"),
        new Fields("key-rename"), new CustomBuffer(new Fields("result")));

    CascadingUtil.get().getFlowConnector().connect(sources, sink, results).complete();

    TupleEntryIterator iter = sink.openForRead(CascadingUtil.get().getFlowProcess());

    assertEquals(new Tuple(1, 108), iter.next().getTuple());
    assertEquals(new Tuple(2, 5), iter.next().getTuple());
    assertEquals(new Tuple(3, 6), iter.next().getTuple());
    assertEquals(new Tuple(5, 7), iter.next().getTuple());

    assertFalse(iter.hasNext());
  }

  protected static class CustomBuffer extends MultiBuffer {

    private final int toEmit;
    public CustomBuffer(Fields output) {
      super(output);
      toEmit = output.size();
    }

    @Override
    public void operate() {
      int result = 0;
      Iterator<Tuple> c1 = getArgumentsIterator(0);
      System.out.println();
      while (c1.hasNext()) {
        Tuple t = c1.next();
        System.out.println("T: "+t);
        result += t.getInteger(1);
      }

      Iterator<Tuple> c2 = getArgumentsIterator(1);
      while (c2.hasNext()) {
        Tuple t = c2.next();
        System.out.println("T2: "+t);
        result += t.getInteger(1);
        result += t.getInteger(2);
      }

      List<Object> results = Lists.newArrayList();
      for(int i = 0; i < toEmit; i++){
        results.add(result);
      }
      emit(new Tuple(results.toArray(new Object[results.size()])));
    }

  }
}
