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
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.counters.Counters;
import com.liveramp.cascading_ext.tap.NullTap;

public class TestFilterStats extends BaseTestCase {

  @Test
  public void run() throws IOException {

    Tap source = new MemorySourceTap(Arrays.asList(
        new Tuple("A", true),
        new Tuple("B", false)),
        new Fields("field1", "field2"));

    Pipe pipe = new Pipe("pipe");
    pipe = new Each(pipe, new FilterStats(new MyFilter()));

    Flow f = CascadingUtil.get().getFlowConnector().connect(source, new NullTap(), pipe);
    f.complete();

    Assert.assertEquals(2l, Counters.get(f, "TestFilterStats.java", "53 - MyFilter - Input records").longValue());
    Assert.assertEquals(1l, Counters.get(f, "TestFilterStats.java", "53 - MyFilter - Kept records").longValue());
    Assert.assertEquals(1l, Counters.get(f, "TestFilterStats.java", "53 - MyFilter - Removed records").longValue());
  }

  private static class MyFilter extends BaseOperation<NoContext> implements Filter<NoContext> {
    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
      return !filterCall.getArguments().getBoolean("field2");
    }
  }
}
