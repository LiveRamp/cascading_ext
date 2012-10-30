package com.liveramp.cascading_ext.example;

import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import com.liveramp.cascading_ext.CascadingUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * An example of a simple flow that is connected and run using a default instance of CascadingUtil.
 */
public class SimpleFlowExample {
  public static void main(String[] args) throws IOException {
    if(args.length != 1){
      System.out.println("Usage: hadoop jar cascading_ext.job.jar com.liveramp.cascading_ext.example.SimpleFlowExample <output dir>");
      return;
    }

    String outputDir = args[0];
    Tap sink = new Hfs(new SequenceFile(new Fields("field1", "field2", "field3", "field4")), outputDir);

    Pipe source1 = new Pipe("source1");

    Pipe source2 = new Pipe("source2");

    Pipe joined = new CoGroup(source1, new Fields("field1"),
        source2, new Fields("field3"));

    Map<String, Tap> sources = new HashMap<String, Tap>();
    sources.put("source1", ExampleFixtures.SOURCE_TAP_1);
    sources.put("source2", ExampleFixtures.SOURCE_TAP_2);

    CascadingUtil.get().getFlowConnector().connect("Example flow", sources, sink, joined).complete();

    //  Take a look at the output tuples
    TupleEntryIterator output = sink.openForRead(CascadingUtil.get().getFlowProcess());
    System.out.println("Output tuples from flow:");
    while(output.hasNext()){
      System.out.println(output.next().getTuple());
    }
  }
}
