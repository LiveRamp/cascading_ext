package com.liveramp.cascading_ext.assembly;

import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.tap.TapHelper;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestRenameInPlace extends BaseTestCase {

  @Test
  public void testIt() throws IOException {

    Hfs source1 = new Hfs(new SequenceFile(new Fields("key", "num")), getTestRoot()+"/input");
    TapHelper.writeToTap(source1,
        new Tuple(3, 4));

    Hfs sink = new Hfs(new SequenceFile(new Fields("key2", "num")), getTestRoot()+"/output");

    Pipe input = new Pipe("input");
    input = new RenameInPlace(input, new Fields("key", "num"), new Fields("key"), new Fields("key2"));


    CascadingUtil.get().getFlowConnector().connect(source1, sink, input).complete();

    assertEquals(new Tuple(3, 4), sink.openForRead(CascadingUtil.get().getFlowProcess()).next().getTuple());

    System.out.println(TapHelper.getAllTuples(sink));
  }

}
