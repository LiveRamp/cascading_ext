/**
 *  Copyright 2012 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.liveramp.cascading_ext.operation;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.*;
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

public class TestFunctionStats extends BaseTestCase {

  @Test
  public void run() {

    Tap source = new MemorySourceTap(Arrays.asList(
        new Tuple("A", true),
        new Tuple("B", false)),
        new Fields("field1", "field2"));

    Pipe pipe = new Pipe("pipe");
    pipe = new Each(pipe, new FunctionStats<NoContext>(new MyFunction()));
    pipe = new Each(pipe, new MyFilter());

    Flow f = CascadingUtil.get().getFlowConnector().connect(source, new NullTap(), pipe);
    f.complete();

    FlowStats fs = f.getFlowStats();

    Assert.assertEquals(2l, Counters.get(fs, OperationStatsUtils.COUNTER_CATEGORY, "MyFunction - Input records").longValue());
    Assert.assertEquals(4l, Counters.get(fs, OperationStatsUtils.COUNTER_CATEGORY, "MyFunction - Output records").longValue());
  }

  private static class MyFilter extends BaseOperation<NoContext> implements Filter<NoContext> {
    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
      System.out.println(filterCall.getArguments());
      return false;
    }
  }

  private static class MyFunction extends BaseOperation<NoContext> implements Function<NoContext> {

    public MyFunction() {
      super(new Fields("field3"));
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<NoContext> noContextFunctionCall) {
      noContextFunctionCall.getOutputCollector().add(new Tuple(1));
      noContextFunctionCall.getOutputCollector().add(new Tuple(2));
    }
  }
}
