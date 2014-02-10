package com.liveramp.cascading_ext.combiner;

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
import com.liveramp.cascading_ext.combiner.lib.ExactAggregatorDefinition;
import com.liveramp.cascading_ext.combiner.lib.MultiExactAggregator;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TestMultiExactAggregator extends BaseTestCase {

  private final String INPUT_PATH = getTestRoot() + "/input";
  private final String OUTPUT_PATH = getTestRoot() + "/output";

  @Test
  public void testMain() throws IOException {

    Hfs source = new Hfs(new SequenceFile(new Fields("key", "a", "b")), INPUT_PATH);
    TupleEntryCollector tc = source.openForWrite(CascadingUtil.get().getFlowProcess());
    tc.add(new Tuple("k0", 1, 1));
    tc.add(new Tuple("k0", 2, 5));
    tc.add(new Tuple("k1", 1, 7));
    tc.add(new Tuple("k1", -2, 10));
    tc.add(new Tuple("k1", -2, -9));
    tc.close();

    Tap sink = new Hfs(new SequenceFile(new Fields("key", "a_sum", "b_sum", "a_b_sum", "a_b_sum_doubled")), OUTPUT_PATH);

    Pipe pipe = new Pipe("pipe");
    pipe = Combiner.assembly(pipe,
        new MultiExactAggregator(
            new ExactAggregatorDefinition(new Fields("a"), new Fields("a_sum"), new Aggregator()),
            new ExactAggregatorDefinition(new Fields("b"), new Fields("b_sum"), new Aggregator()),
            new ExactAggregatorDefinition(new Fields("a", "b"), new Fields("a_b_sum"), new OtherAggregator())),
        new Fields("key"),
        new Fields("a", "b"),
        new Fields("a_sum", "b_sum", "a_b_sum", "a_b_sum_doubled"));

    CascadingUtil.get().getFlowConnector().connect(source, sink, pipe).complete();

    List<Tuple> tuples = getAllTuples(sink);

    printCollection(tuples);

    assertCollectionEquivalent(Arrays.asList(
        new Tuple("k0", 3L, 6L, 9L, 18L),
        new Tuple("k1", -3L, 8L, 5L, 10L)),
        tuples);
  }

  private static class Aggregator extends BaseExactAggregator<Long> implements ExactAggregator<Long> {

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
      return aggregate + partialAggregate.getLong(0);
    }

    @Override
    public Tuple toTuple(Long aggregate) {
      return new Tuple(aggregate);
    }
  }

  private static class OtherAggregator extends BaseExactAggregator<Long> implements ExactAggregator<Long> {

    @Override
    public Long initialize() {
      return 0L;
    }

    @Override
    public Long partialAggregate(Long aggregate, TupleEntry nextValue) {
      return aggregate + nextValue.getLong("a") + nextValue.getLong("b");
    }

    @Override
    public Long finalAggregate(Long aggregate, TupleEntry partialAggregate) {
      return aggregate + partialAggregate.getLong("a_b_sum");
    }

    @Override
    public Tuple toTuple(Long aggregate) {
      return new Tuple(aggregate, aggregate * 2);
    }
  }
}
