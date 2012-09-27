package com.liveramp.cascading_ext.tap;

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.twitter.maple.tap.MemorySourceTap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

import java.io.IOException;

/**
 * @author eddie
 */
public class TestNullTap extends BaseTestCase {
  public void testWrite() throws IOException {
    TupleEntryCollector tc = new NullTap().openForWrite(CascadingUtil.get().getFlowProcess());
    tc.add(new Tuple("testing if it fails"));
  }

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
