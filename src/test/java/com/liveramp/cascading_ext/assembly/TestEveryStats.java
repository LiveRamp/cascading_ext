package com.liveramp.cascading_ext.assembly;

import java.io.IOException;
import java.util.Arrays;

import com.twitter.maple.tap.MemorySourceTap;
import org.junit.Assert;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.counters.Counters;
import com.liveramp.cascading_ext.tap.NullTap;

public class TestEveryStats extends BaseTestCase {

  @Test
  public void run() throws IOException {
    Tap source = new MemorySourceTap(Arrays.asList(
        new Tuple(1),
        new Tuple(2),
        new Tuple(1)),
        new Fields("field"));

    Pipe pipe = new Pipe("pipe");
    pipe = new GroupBy(pipe, new Fields("field"));
    pipe = new EveryStats(pipe, new MyBuffer());

    Flow f = CascadingUtil.get().getFlowConnector().connect(source, new NullTap(), pipe);
    f.complete();

    Assert.assertEquals(2l, Counters.get(f, "TestEveryStats.java", "38 - MyBuffer - Input groups").longValue());
    Assert.assertEquals(4l, Counters.get(f, "TestEveryStats.java", "38 - MyBuffer - Output records").longValue());
  }

  private static class MyBuffer extends BaseOperation implements Buffer {

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
      bufferCall.getOutputCollector().add(new Tuple(0));
      bufferCall.getOutputCollector().add(new Tuple(1));
    }
  }
}
