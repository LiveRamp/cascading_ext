//package com.liveramp.cascading_ext;
//
//import cascading.pipe.CoGroup;
//import cascading.pipe.Every;
//import cascading.pipe.Pipe;
//import cascading.pipe.joiner.BufferJoin;
//import cascading.scheme.hadoop.SequenceFile;
//import cascading.tap.Tap;
//import cascading.tap.hadoop.Hfs;
//import cascading.tuple.Fields;
//import cascading.tuple.Tuple;
//import cascading.tuple.TupleEntryIterator;
//import com.liveramp.cascading_ext.multi_group_by.MultiBuffer2;
//import com.liveramp.cascading_ext.multi_group_by.MultiBufferOperation;
//import com.liveramp.cascading_ext.tap.TapHelper;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//
//import static org.junit.Assert.assertFalse;
//
//public class TestBufferJoinExample extends BaseTestCase {
//
//  private Hfs source1;
//  private Hfs source2;
//
//  private final String SOURCE1 = getTestRoot() + "/mgb_source1";
//  private final String SOURCE2 = getTestRoot() + "/mgb_source2";
//
//  @Before
//  public void setUp() throws IOException {
//
//    source1 = new Hfs(new SequenceFile(new Fields("key", "num")), SOURCE1);
//    TapHelper.writeToTap(source1,
//        new Tuple(1, 1),
//        new Tuple(1, 3),
//        new Tuple(1, 2),
//        new Tuple(2, 5),
//        new Tuple(3, 3),
//        new Tuple(3, 3));
//
//    source2 = new Hfs(new SequenceFile(new Fields("key", "num1", "num2")), SOURCE2);
//    TapHelper.writeToTap(source2,
//        new Tuple(1, 101, 1),
//        new Tuple(5, 5, 2),
//        new Tuple(3, 0, 0));
//  }
//
//  @Test
//  public void testSimple3() throws Exception {
//
//    final String OUTPUT = getTestRoot() + "/mgb_output";
//
//    Hfs sink = new Hfs(new SequenceFile(new Fields("my_field")), OUTPUT);
//
//    Map<String, Tap> sources = new HashMap<String, Tap>();
//    sources.put("s1", source1);
//    sources.put("s2", source2);
//
//    Pipe s1 = new Pipe("s1");
//    Pipe s2 = new Pipe("s2");
//
//    Pipe results = new CoGroup(s1, new Fields("key"),
//        s2, new Fields("key"),
//        new BufferJoin()
//    );
//
//    results = new Every(results,
//        new MultiBufferOperation(new MyMultiBuffer())
//    );
//
//    CascadingUtil.get().getFlowConnector().connect(sources, sink, results).complete();
//
//    TupleEntryIterator iter = sink.openForRead(CascadingUtil.get().getFlowProcess());
//
//    while(iter.hasNext()){
//      System.out.println(iter.next());
//    }
//
////    assertEquals(new Tuple(1, 108, 108, 108, 108, 108, 108), iter.next().getTuple());
////    assertEquals(new Tuple(2, 5, 5, 5, 5, 5, 5), iter.next().getTuple());
////    assertEquals(new Tuple(3, 6, 6, 6, 6, 6, 6), iter.next().getTuple());
////    assertEquals(new Tuple(5, 7, 7, 7, 7, 7, 7), iter.next().getTuple());
//
//    assertFalse(iter.hasNext());
//  }
//
//  private static class MyMultiBuffer extends MultiBuffer2 {
//
//    public MyMultiBuffer() {
//      super(new Fields("my_field"));
//    }
//
//    @Override
//    public void operate() {
//
//      Iterator<Tuple> args1 = getArgumentsIterator(0);
//      Iterator<Tuple> args2 = getArgumentsIterator(1);
//
//      System.out.println();
//      while(args1.hasNext()){
//        System.out.println(args1.next());
//      }
//      System.out.println();
//      while(args2.hasNext()){
//        System.out.println(args2.next());
//      }
//
//    }
//  }
//
//}
