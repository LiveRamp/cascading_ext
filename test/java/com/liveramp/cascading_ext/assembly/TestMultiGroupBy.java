package com.liveramp.cascading_ext.assembly;

import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.multi_group_by.MultiBuffer;
import com.liveramp.cascading_ext.tap.TapHelper;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;

public class TestMultiGroupBy extends BaseTestCase {

  @Test
  public void testSimple() throws Exception {
    final String SOURCE1 = getTestRoot() + "/mgb_source1";
    final String SOURCE2 = getTestRoot() + "/mgb_source2";
    final String OUTPUT = getTestRoot() + "/mgb_output";

    Hfs source1 = new Hfs(new SequenceFile(new Fields("key", "num")), SOURCE1);
    TapHelper.writeToTap(source1,
        new Tuple(1, 1),
        new Tuple(1, 3),
        new Tuple(1, 2),
        new Tuple(2, 5),
        new Tuple(3, 3),
        new Tuple(3, 3));

    Hfs source2 = new Hfs(new SequenceFile(new Fields("key", "num1", "num2")), SOURCE2);
    TapHelper.writeToTap(source2,
        new Tuple(1, 101, 1),
        new Tuple(5, 5, 2),
        new Tuple(3, 0, 0));

    Hfs sink = new Hfs(new SequenceFile(new Fields("key-rename", "result", " result1", "result2", "result3", "result4", "result5")), OUTPUT);

    Map<String, Tap> sources = new HashMap<String, Tap>();
    sources.put("s1", source1);
    sources.put("s2", source2);

    Pipe s1 = new Pipe("s1");
    Pipe s2 = new Pipe("s2");

    Pipe results = new MultiGroupBy(s1, new Fields("key"), s2, new Fields("key"),
        new Fields("key-rename"), new CustomBuffer());

    CascadingUtil.get().getFlowConnector().connect(sources, sink, results).complete();

    TupleEntryIterator iter = sink.openForRead(CascadingUtil.get().getFlowProcess());

    assertEquals(new Tuple(1, 108, 108, 108, 108, 108, 108), iter.next().getTuple());
    assertEquals(new Tuple(2, 5, 5, 5, 5, 5, 5), iter.next().getTuple());
    assertEquals(new Tuple(3, 6, 6, 6, 6, 6, 6), iter.next().getTuple());
    assertEquals(new Tuple(5, 7, 7, 7, 7, 7, 7), iter.next().getTuple());

    assertFalse(iter.hasNext());

  }

  protected static class CustomBuffer extends MultiBuffer {
    public CustomBuffer() {
      super(new Fields("result", " result1", "result2", "result3", "result4", "result5"));
    }

    @Override
    public void operate() {
      int result = 0;
      Iterator<Tuple> c1 = getArgumentsIterator(0);
      while (c1.hasNext()) {
        Tuple t = c1.next();
        result += t.getInteger(1);
      }

      Iterator<Tuple> c2 = getArgumentsIterator(1);
      while (c2.hasNext()) {
        Tuple t = c2.next();
        result += t.getInteger(1);
        result += t.getInteger(2);
      }

      emit(new Tuple(result, result, result, result, result, result));
    }

  }
}
