package com.liveramp.cascading_ext.operation;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import com.twitter.maple.tap.MemorySourceTap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;

import static org.junit.Assert.assertEquals;

public class TestBatchFunction extends BaseTestCase {

  public static final Fields OUT_FIELD = new Fields("out");
  public static final Charset CHARSET = Charset.forName("UTF-8");

  @Test
  public void testFunc() throws IOException {
    Tap src = new MemorySourceTap(Lists.newArrayList(
        new Tuple(makeBytes("1")),
        new Tuple(makeBytes("2")),
        new Tuple(makeBytes("3")),
        new Tuple(makeBytes("4")),
        new Tuple(makeBytes("5")),
        new Tuple(makeBytes("6"))),
      new Fields("in"));

    Pipe pipe = new Pipe("batch-pipe");
    pipe = new Each(pipe, new AnnotateWithSizeOfCurrentBatch(4));

    Tap<Configuration, RecordReader, OutputCollector> dst = new Lfs(new SequenceFile(OUT_FIELD), getTestRoot()+"/out");

    Flow f = CascadingUtil.get().getFlowConnector().connect(src, dst, pipe);
    f.complete();

    TupleEntryIterator tupleEntryIterator = dst.openForRead(CascadingUtil.get().getFlowProcess());
    List<BytesWritable> outStrings = new ArrayList<BytesWritable>(6);
    while (tupleEntryIterator.hasNext()) {
      TupleEntry next = tupleEntryIterator.next();
      BytesWritable bytes = (BytesWritable) next.getObject(OUT_FIELD);
      outStrings.add(bytes);
    }

    assertEquals(outStrings,
        Lists.newArrayList(makeBytes("1-4"),
            makeBytes("2-4"),
            makeBytes("3-4"),
            makeBytes("4-4"),
            makeBytes("5-2"),
            makeBytes("6-2")));
  }

  private BytesWritable makeBytes(String s) {
    return new BytesWritable(s.getBytes(CHARSET));
  }

  private static class AnnotateWithSizeOfCurrentBatch extends BatchFunction<BytesWritable, BytesWritable> {

    public AnnotateWithSizeOfCurrentBatch(int batchSize) {
      super(OUT_FIELD, batchSize);
    }

    @Override
    public List<BytesWritable> apply(FlowProcess flowProcess, List<BytesWritable> input) {
      List<BytesWritable> output = Lists.newArrayList();

      for (BytesWritable bytesWritable : input) {
        String in = new String(bytesWritable.getBytes(), CHARSET);
        String out = in + "-" + input.size();
        output.add(new BytesWritable(out.getBytes(CHARSET)));
      }

      return output;
    }
  }

}
