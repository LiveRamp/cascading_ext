package com.liveramp.cascading_ext.assembly;

import cascading.flow.Flow;
import cascading.pipe.Pipe;
import cascading.stats.FlowStats;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.common.collect.Lists;
import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.counters.Counters;
import com.liveramp.cascading_ext.tap.NullTap;
import com.twitter.maple.tap.MemorySourceTap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestIncrementForFieldValues extends BaseTestCase {

  private static enum Counter {
    B
  }

  @Test
  public void run() {
    Tap source = new MemorySourceTap(
        Arrays.asList(
            new Tuple(1),
            new Tuple(2),
            new Tuple(2),
            new Tuple((Integer) null),
            new Tuple((Integer) null),
            new Tuple((Integer) null)),
        new Fields("field"));

    Pipe pipe = new Pipe("pipe");
    pipe = new IncrementForFieldValues(pipe, "Group", "CounterA", new Fields("field"), Lists.<Integer>newArrayList(2));
    pipe = new IncrementForFieldValues(pipe, Counter.B, new Fields("field"), Lists.<Object>newArrayList((Object) null));

    Flow f = CascadingUtil.get().getFlowConnector().connect(source, new NullTap(), pipe);
    f.complete();

    FlowStats fs = f.getFlowStats();

    Assert.assertEquals(2l, Counters.get(fs, "Group", "CounterA").longValue());
    Assert.assertEquals(3l, Counters.get(fs, Counter.B).longValue());


  }

  @Test
  public void runMultiValue() {
    Tap source = new MemorySourceTap(
        Arrays.asList(
            new Tuple(1, 1),
            new Tuple(1, 2),
            new Tuple(1, 1),
            new Tuple((Integer) null, (Integer) 2)),
        new Fields("key", "value"));

    Pipe pipe = new Pipe("pipe");
    pipe = new IncrementForFieldValues(pipe, "Group", "CounterA", new Fields("key", "value"), Lists.<Integer>newArrayList(1, 1));
    pipe = new IncrementForFieldValues(pipe, Counter.B, new Fields("key", "value"), Lists.<Object>newArrayList((Object) null, 2));

    Flow f = CascadingUtil.get().getFlowConnector().connect(source, new NullTap(), pipe);
    f.complete();

    FlowStats fs = f.getFlowStats();

    Assert.assertEquals(2l, Counters.get(fs, "Group", "CounterA").longValue());
    Assert.assertEquals(1l, Counters.get(fs, Counter.B).longValue());


  }


}
