package com.liveramp.cascading_ext.assembly;

import cascading.cascade.Cascades;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.multi_group_by.MultiBuffer;
import org.apache.hadoop.fs.FileSystem;

import java.util.Iterator;
import java.util.Map;

/**
 * @author eddie
 */
public class TestMultiGroupBy extends BaseTestCase {
  FileSystem fs;

  public TestMultiGroupBy() throws Exception {
    super();
    fs = FileSystemHelper.getFS();
  }

  public void testSimple() throws Exception {
    final String SOURCE1 = getTestRoot()+"/mgb_source1";
    final String SOURCE2 = getTestRoot()+"/mgb_source2";
    final String OUTPUT = getTestRoot()+"/mgb_output";

    Tap source1 = new Hfs(new SequenceFile(new Fields("key", "num")), SOURCE1);

    TupleEntryCollector coll = source1.openForWrite(CascadingUtil.get().getFlowProcess());
    coll.add(new Tuple(1, 1));
    coll.add(new Tuple(1, 3));
    coll.add(new Tuple(1, 2));
    coll.add(new Tuple(2, 5));
    coll.add(new Tuple(3, 3));
    coll.add(new Tuple(3, 3));
    coll.close();

    Tap source2 = new Hfs(new SequenceFile(new Fields("key2", "num1", "num2")), SOURCE2);
    coll = source2.openForWrite(CascadingUtil.get().getFlowProcess());
    coll.add(new Tuple(1, 101, 1));
    coll.add(new Tuple(5, 5, 2));
    coll.add(new Tuple(3, 0, 0));
    coll.close();

    Pipe s1 = new Pipe("s1");

    Pipe s2 = new Pipe("s2");

    Pipe results = new MultiGroupBy(new Pipe[] {s1, s2}, new Fields[] {new Fields("key"),
            new Fields("key2")}, new Fields("key"), new AddEmUp());

    Tap sink = new Hfs(new SequenceFile(new Fields("key", "result"," result1", "result2", "result3", "result4", "result5")), OUTPUT);

    Map<String, Tap> sources = Cascades.tapsMap(new Pipe[]{s1, s2},
            new Tap[]{source1, source2});

    CascadingUtil.get().getFlowConnector().connect(sources, sink, results).complete();

    TupleEntryIterator iter = sink.openForRead(CascadingUtil.get().getFlowProcess());

    assertEquals(new Tuple(1, 108, 108, 108, 108, 108, 108), iter.next().getTuple());
    assertEquals(new Tuple(2, 5, 5, 5, 5, 5, 5), iter.next().getTuple());
    assertEquals(new Tuple(3, 6, 6, 6, 6, 6, 6), iter.next().getTuple());
    assertEquals(new Tuple(5, 7, 7, 7, 7, 7, 7), iter.next().getTuple());

    assertFalse(iter.hasNext());

  }

  protected static class AddEmUp extends MultiBuffer {
    public AddEmUp() {
      super(new Fields("result"," result1", "result2", "result3", "result4", "result5"));
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
