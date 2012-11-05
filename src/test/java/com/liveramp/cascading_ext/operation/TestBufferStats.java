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
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
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

public class TestBufferStats extends BaseTestCase {

  @Test
  public void run() {

    Tap source = new MemorySourceTap(Arrays.asList(
        new Tuple("A", true),
        new Tuple("A", true),
        new Tuple("A", true),
        new Tuple("B", false)),
        new Fields("field1", "field2"));


    Pipe input = new Pipe("input");
    input = new GroupBy(input, new Fields("field1"));
    input = new Every(input, new BufferStats<NoContext>(new MyBuffer()));

    Flow f = CascadingUtil.get().getFlowConnector().connect(source, new NullTap(), input);
    f.complete();
    FlowStats stats = f.getFlowStats();

    Assert.assertEquals(2l, Counters.get(stats, OperationStatsUtils.COUNTER_CATEGORY, "MyBuffer - Output records").longValue());
    Assert.assertEquals(2l, Counters.get(stats, OperationStatsUtils.COUNTER_CATEGORY, "MyBuffer - Input groups").longValue());
  }

  public static class MyBuffer extends BaseOperation<NoContext> implements Buffer<NoContext> {

    @Override
    public void operate(FlowProcess flowProcess, BufferCall<NoContext> noContextBufferCall) {
      noContextBufferCall.getOutputCollector().add(new Tuple("1"));
    }
  }
}
