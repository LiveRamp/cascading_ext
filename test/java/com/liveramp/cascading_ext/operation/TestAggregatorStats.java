package com.liveramp.cascading_ext.operation;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.stats.FlowStats;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.counters.Counters;
import com.liveramp.cascading_ext.tap.NullTap;
import com.twitter.maple.tap.MemorySourceTap;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestAggregatorStats extends BaseTestCase {

  @Test
  public void run(){

    Tap source = new MemorySourceTap(Arrays.asList(
        new Tuple("A", true),
        new Tuple("A", true),
        new Tuple("A", true),
        new Tuple("B", false)),
        new Fields("field1", "field2"));

    Pipe input = new Pipe("input");
    input = new GroupBy(input, new Fields("field1"));
    input = new Every(input, new AggregatorStats<MyAggregator.Context>(new MyAggregator()));

    Flow f = CascadingUtil.get().getFlowConnector().connect(source, new NullTap(), input);
    f.complete();
    FlowStats stats = f.getFlowStats();

    Assert.assertEquals(4l, Counters.get(stats, OperationStatsUtils.COUNTER_CATEGORY, "MyAggregator - Input records").longValue());
    Assert.assertEquals(2l, Counters.get(stats, OperationStatsUtils.COUNTER_CATEGORY, "MyAggregator - Total output records").longValue());
  }

  private static class MyAggregator extends BaseOperation<MyAggregator.Context> implements Aggregator<MyAggregator.Context> {

    public MyAggregator(){
      super(new Fields("output"));
    }

    public static class Context {}

    @Override
    public void start(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
      aggregatorCall.setContext(new Context());
    }

    @Override
    public void aggregate(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
    }

    @Override
    public void complete(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
      aggregatorCall.getOutputCollector().add(new Tuple("1"));
    }
  }
}
