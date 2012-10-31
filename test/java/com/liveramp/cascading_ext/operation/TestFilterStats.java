package com.liveramp.cascading_ext.operation;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
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
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestFilterStats extends BaseTestCase {

  @Test
  public void run(){

    Tap source = new MemorySourceTap(Arrays.asList(
          new Tuple("A", true),
          new Tuple("B", false)),
        new Fields("field1", "field2"));

    Pipe pipe = new Pipe("pipe");
    pipe = new Each(pipe, new FilterStats<NoContext>(new MyFilter()));

    Flow f = CascadingUtil.get().getFlowConnector().connect(source, new NullTap(), pipe);
    f.complete();

    FlowStats fs = f.getFlowStats();

    Assert.assertEquals(2l, Counters.get(fs, OperationStatsUtils.COUNTER_CATEGORY, "MyFilter - Input records").longValue());
    Assert.assertEquals(1l, Counters.get(fs, OperationStatsUtils.COUNTER_CATEGORY, "MyFilter - Accepted records").longValue());
    Assert.assertEquals(1l, Counters.get(fs, OperationStatsUtils.COUNTER_CATEGORY, "MyFilter - Rejected records").longValue());
  }

  private static class MyFilter extends BaseOperation<NoContext> implements Filter<NoContext> {
    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
      return !filterCall.getArguments().getBoolean("field2");
    }
  }
}
