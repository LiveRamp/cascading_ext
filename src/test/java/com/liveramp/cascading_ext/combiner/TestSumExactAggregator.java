package com.liveramp.cascading_ext.combiner;

import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.combiner.lib.SumExactAggregator;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TestSumExactAggregator extends BaseTestCase {

  private final String INPUT_PATH = getTestRoot() + "/input";
  private final String OUTPUT_PATH = getTestRoot() + "/output";

  @Test
  public void testMain() throws IOException {
    Hfs source = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH);
    TupleEntryCollector tc = source.openForWrite(CascadingUtil.get().getFlowProcess());
    tc.add(new Tuple("k0", 1));
    tc.add(new Tuple("k0", 2));
    tc.add(new Tuple("k1", 10));
    tc.add(new Tuple("k1", -2));
    tc.add(new Tuple("k1", -9));
    tc.add(new Tuple("k2", -3));
    tc.add(new Tuple("k2", -6));
    tc.close();

    Tap sink = new Hfs(new SequenceFile(new Fields("key", "sum")), OUTPUT_PATH);
    
    Pipe pipe = new Pipe("pipe");
    pipe = Combiner.assembly(pipe, new SumExactAggregator(1), new Fields("key"), new Fields("value"), new Fields("sum"));

    CascadingUtil.get().getFlowConnector().connect(source, sink, pipe).complete();

    List<Tuple> tuples = getAllTuples(sink);

    printCollection(tuples);

    assertCollectionEquivalent(Arrays.asList(
        new Tuple("k0", 3L), new Tuple("k1", -1L), new Tuple("k2", -9L)), tuples);
  }
}
