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

package com.liveramp.cascading_ext.tap;

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import com.google.common.collect.Lists;
import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.twitter.maple.tap.MemorySourceTap;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;


public class TestNullTap extends BaseTestCase {

  @Test
  public void testWrite() throws IOException {
    TupleEntryCollector tc = new NullTap().openForWrite(CascadingUtil.get().getFlowProcess());
    tc.add(new Tuple("testing if it fails"));
  }

  @Test
  public void testFlow() throws IOException {
    Tap input = new MemorySourceTap(
        Lists.newArrayList(
            new Tuple("line1", 1),
            new Tuple("line2", 2),
            new Tuple("line3", 3),
            new Tuple("line4", 4)),
        new Fields("description", "count"));

    Tap output = new NullTap();

    Pipe pipe = new Pipe("pipe");
    pipe = new Each(pipe, Fields.ALL, new Identity(), Fields.RESULTS);

    CascadingUtil.get().getFlowConnector().connect(input, output, pipe).complete();

    assertFalse(fs.exists(new Path(output.getIdentifier())));
  }
}
