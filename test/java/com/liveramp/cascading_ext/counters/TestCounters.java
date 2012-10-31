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

package com.liveramp.cascading_ext.counters;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.FileSystemHelper;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author eddie
 */
public class TestCounters extends BaseTestCase {

  @Test
  public void testBlackHoleWarning() throws IOException {

    FileSystemHelper.safeMkdirs(FileSystemHelper.getFS(), new Path(getTestRoot() + "/input"));
    Tap input = new Hfs(new TextLine(), getTestRoot() + "/input");
    Tap output = new Hfs(new TextLine(), getTestRoot() + "/output", SinkMode.REPLACE);
    Pipe removeAll = new Pipe("remove all");
    removeAll = new Each(removeAll, new RemoveAll());

    Flow flow = CascadingUtil.get().getFlowConnector().connect(input, output, removeAll);
    flow.complete();
    assertTrue("no warning on empty input", !Counters.prettyCountersString(flow).contains("BLACK HOLE WARNING"));

    FileSystemHelper.createFile(FileSystemHelper.getFS(), getTestRoot() + "/input/important_messages", "hi vlad");

    flow = CascadingUtil.get().getFlowConnector().connect(input, output, removeAll);
    flow.complete();
    assertTrue("warning on empty output and nonempty input", Counters.prettyCountersString(flow).contains("BLACK HOLE WARNING"));

    flow = CascadingUtil.get().getFlowConnector().connect(input, output, new Pipe("identity"));
    flow.complete();
    assertTrue("no warning on nonempty output", !Counters.prettyCountersString(flow).contains("BLACK HOLE WARNING"));
  }

  @SuppressWarnings({"rawtypes", "serial"})
  private static class RemoveAll extends BaseOperation implements Filter {
    public boolean isRemove(FlowProcess arg0, FilterCall arg1) {
      return true;
    }
  }
}
