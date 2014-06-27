package com.liveramp.cascading_ext.combiner;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.combiner.lib.BaseExactAggregator;
import com.liveramp.cascading_ext.util.SimpleTupleMemoryUsageEstimator;
import com.liveramp.commons.collections.MemoryBoundLruHashMap;
import com.liveramp.commons.util.LongMemoryUsageEstimator;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TestCombiner extends BaseTestCase {

  private final String INPUT_PATH = getTestRoot() + "/input";
  private final String OUTPUT_PATH = getTestRoot() + "/output";

  @Test
  public void testSimpleCombiner() throws IOException {

    Hfs source = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH);
    TupleEntryCollector tc = source.openForWrite(CascadingUtil.get().getFlowProcess());
    tc.add(new Tuple("k0", 1));
    tc.add(new Tuple("k0", 2));
    tc.add(new Tuple("k1", 1));
    tc.add(new Tuple("k1", -3));
    tc.add(new Tuple("k1", 10));
    tc.close();

    Tap sink = new Hfs(new SequenceFile(new Fields("key", "sum")), OUTPUT_PATH);

    Pipe pipe = new Pipe("pipe");
    pipe = Combiner.assembly(pipe, new SimpleAggregator(), new Fields("key"), new Fields("value"), new Fields("sum"));

    CascadingUtil.get().getFlowConnector().connect(source, sink, pipe).complete();

    List<Tuple> tuples = getAllTuples(sink);

    printCollection(tuples);

    assertCollectionEquivalent(Arrays.asList(
            new Tuple("k0", 3L),
            new Tuple("k1", 8L)),
        tuples
    );
  }


  @Test
  public void testSimpleCombinerWithMemoryLimit() throws IOException {

    Hfs source = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH);
    TupleEntryCollector tc = source.openForWrite(CascadingUtil.get().getFlowProcess());
    tc.add(new Tuple("key0", 1));
    tc.add(new Tuple("key0", 2));
    tc.add(new Tuple("key1", 1));
    tc.add(new Tuple("key1", -3));
    tc.add(new Tuple("key0", 10));
    tc.close();

    Tap sink = new Hfs(new SequenceFile(new Fields("key", "sum")), OUTPUT_PATH);

    Pipe pipe = new Pipe("pipe");
    pipe = new Each(pipe, Combiner.function(new SimpleAggregator(), new Fields("key"), new Fields("value"),
        new Fields("sum"), MemoryBoundLruHashMap.UNLIMITED_ITEM_CAPACITY, 100, new SimpleTupleMemoryUsageEstimator(), new LongMemoryUsageEstimator(), false));

    CascadingUtil.get().getFlowConnector().connect(source, sink, pipe).complete();

    List<Tuple> tuples = getAllTuples(sink);

    printCollection(tuples);

    assertCollectionEquivalent(Arrays.asList(
            new Tuple("key0", 3L),
            new Tuple("key1", -2L),
            new Tuple("key0", 10L)),
        tuples
    );
  }

  private static class SimpleAggregator extends BaseExactAggregator<Long> implements ExactAggregator<Long> {

    @Override
    public Long initialize() {
      return 0L;
    }

    @Override
    public Long partialAggregate(Long aggregate, TupleEntry nextValue) {
      return aggregate + nextValue.getLong(0);
    }

    @Override
    public Long finalAggregate(Long aggregate, TupleEntry partialAggregate) {
      return partialAggregate(aggregate, partialAggregate);
    }

    @Override
    public Tuple toTuple(Long aggregate) {
      return new Tuple(aggregate);
    }
  }

  @Test
  public void testComplexCombiner() throws IOException {

    Hfs source = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH);
    TupleEntryCollector tc = source.openForWrite(CascadingUtil.get().getFlowProcess());
    tc.add(new Tuple("k0", 1));
    tc.add(new Tuple("k0", 2));
    tc.add(new Tuple("k1", 1));
    tc.add(new Tuple("k1", 4));
    tc.add(new Tuple("k1", 10));
    tc.close();

    Tap sink = new Hfs(new SequenceFile(new Fields("key", "sum", "num_values", "average")), OUTPUT_PATH);

    Pipe pipe = new Pipe("pipe");
    pipe = Combiner.assembly(pipe,
        new ComplexAggregator(),
        new Fields("key"),
        new Fields("value"),
        new Fields("sum", "num_values"),
        new Fields("sum", "num_values", "average"));

    CascadingUtil.get().getFlowConnector().connect(source, sink, pipe).complete();

    List<Tuple> tuples = getAllTuples(sink);

    printCollection(tuples);

    assertCollectionEquivalent(Arrays.asList(
            new Tuple("k0", 3L, 2L, 1.5D),
            new Tuple("k1", 15L, 3L, 5.0D)),
        tuples
    );
  }

  private static class ComplexAggregator implements ExactAggregator<long[]> {

    @Override
    public long[] initialize() {
      return new long[]{0L, 0L};
    }

    @Override
    public long[] partialAggregate(long[] aggregate, TupleEntry nextValue) {
      aggregate[0] += nextValue.getLong(0);
      aggregate[1] += 1;
      return aggregate;
    }

    @Override
    public Tuple toPartialTuple(long[] aggregate) {
      return new Tuple(aggregate[0], aggregate[1]);
    }

    @Override
    public long[] finalAggregate(long[] aggregate, TupleEntry partialAggregate) {
      aggregate[0] += partialAggregate.getLong(0);
      aggregate[1] += partialAggregate.getLong(1);
      return aggregate;
    }

    @Override
    public Tuple toFinalTuple(long[] aggregate) {
      double average = (double)aggregate[0] / aggregate[1];
      return new Tuple(aggregate[0], aggregate[1], average);
    }
  }
}
