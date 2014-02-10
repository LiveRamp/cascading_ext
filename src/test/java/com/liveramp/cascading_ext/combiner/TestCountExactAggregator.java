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
import com.liveramp.cascading_ext.combiner.lib.CountExactAggregator;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TestCountExactAggregator extends BaseTestCase {

  private final String INPUT_PATH = getTestRoot() + "/input";
  private final String OUTPUT_PATH = getTestRoot() + "/output";

  @Test
  public void testMain() throws IOException {
    Hfs source = new Hfs(new SequenceFile(new Fields("key")), INPUT_PATH);
    TupleEntryCollector tc = source.openForWrite(CascadingUtil.get().getFlowProcess());
    tc.add(new Tuple("k0"));
    tc.add(new Tuple("k0"));
    tc.add(new Tuple("k1"));
    tc.add(new Tuple("k1"));
    tc.add(new Tuple("k1"));
    tc.add(new Tuple("k2"));
    tc.add(new Tuple("k2"));
    tc.close();

    Tap sink = new Hfs(new SequenceFile(new Fields("key", "count")), OUTPUT_PATH);
    
    Pipe pipe = new Pipe("pipe");
    pipe = Combiner.assembly(pipe, new CountExactAggregator(), new Fields("key"), new Fields(), new Fields("count"));

    CascadingUtil.get().getFlowConnector().connect(source, sink, pipe).complete();

    List<Tuple> tuples = getAllTuples(sink);

    printCollection(tuples);

    assertCollectionEquivalent(Arrays.asList(
        new Tuple("k0", 2L), new Tuple("k1", 3L), new Tuple("k2", 2L)), tuples);
  }
}
