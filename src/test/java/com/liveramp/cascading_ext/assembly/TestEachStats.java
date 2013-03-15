package com.liveramp.cascading_ext.assembly;

import cascading.flow.Flow;
import cascading.operation.filter.FilterNull;
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

public class TestEachStats extends BaseTestCase {

  @Test
  public void run() {
    Tap source = new MemorySourceTap(Arrays.asList(
        new Tuple(1),
        new Tuple(2),
        new Tuple((Integer) null)),
        new Fields("field"));

    Pipe pipe = new Pipe("pipe");
    pipe = new EachStats(pipe, new FilterNull());

    Flow f = CascadingUtil.get().getFlowConnector().connect(source, new NullTap(), pipe);
    f.complete();

    Assert.assertEquals(3l, Counters.get(f, "TestEachStats.java", "31 - FilterNull - Input records").longValue());
    Assert.assertEquals(2l, Counters.get(f, "TestEachStats.java", "31 - FilterNull - Kept records").longValue());
    Assert.assertEquals(1l, Counters.get(f, "TestEachStats.java", "31 - FilterNull - Removed records").longValue());
  }
}
