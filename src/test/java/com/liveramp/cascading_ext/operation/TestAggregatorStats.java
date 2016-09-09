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

import java.io.IOException;
import java.util.Arrays;

import com.twitter.maple.tap.MemorySourceTap;
import org.junit.Assert;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.counters.Counters;
import com.liveramp.cascading_ext.tap.NullTap;

public class TestAggregatorStats extends BaseTestCase {

  @Test
  public void run() throws IOException {

    Tap source = new MemorySourceTap(Arrays.asList(
        new Tuple("A", true),
        new Tuple("A", true),
        new Tuple("A", true),
        new Tuple("B", false)),
        new Fields("field1", "field2"));

    Pipe input = new Pipe("input");
    input = new GroupBy(input, new Fields("field1"));
    input = new Every(input, new AggregatorStats(new MyAggregator()));

    Flow f = CascadingUtil.get().getFlowConnector().connect(source, new NullTap(), input);
    f.complete();

    Assert.assertEquals(4l, Counters.get(f, "TestAggregatorStats.java", "57 - MyAggregator - Input records").longValue());
    Assert.assertEquals(2l, Counters.get(f, "TestAggregatorStats.java", "57 - MyAggregator - Total output records").longValue());
  }

  private static class MyAggregator extends BaseOperation<MyAggregator.Context> implements Aggregator<MyAggregator.Context> {

    public MyAggregator() {
      super(new Fields("output"));
    }

    public static class Context {
    }

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
