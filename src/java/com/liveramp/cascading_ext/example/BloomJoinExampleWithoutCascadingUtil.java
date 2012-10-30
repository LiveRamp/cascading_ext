package com.liveramp.cascading_ext.example;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.assembly.BloomJoin;
import com.liveramp.cascading_ext.bloom.BloomAssemblyStrategy;
import com.liveramp.cascading_ext.bloom.BloomProps;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * An example of how to use the BloomJoin class if for whatever reason you can't run the job using CascadingUtil.
 * The only additional steps needed are setting some default properties and attaching the FlowStepStrategy manually.
 *
 *  In practice this isn't nearly enough data to warrant using BloomJoin, but it will work fine for a test job.
 */
public class BloomJoinExampleWithoutCascadingUtil {

  public static void main(String[] args) throws IOException {
    if(args.length != 1){
      System.out.println("Usage: hadoop jar cascading_ext.job.jar com.liveramp.cascading_ext.example.BloomJoinExampleWithoutCascadingUtil <output dir>");
      return;
    }

    String outputDir = args[0];
    Hfs sink = new Hfs(new SequenceFile(new Fields("field1", "field2", "field3", "field4")), outputDir);

    Pipe source1 = new Pipe("source1");

    Pipe source2 = new Pipe("source2");

    Pipe joined = new BloomJoin(source1, new Fields("field1"),
        source2, new Fields("field3"));

    Map<String, Tap> sources = new HashMap<String, Tap>();
    sources.put("source1", ExampleFixtures.SOURCE_TAP_1);
    sources.put("source2", ExampleFixtures.SOURCE_TAP_2);

    //  set some default properties and set the flow step strategy
    Flow f = new HadoopFlowConnector(BloomProps.getDefaultProperties()).connect("Example BloomJoin", sources, sink, joined);
    f.setFlowStepStrategy(new BloomAssemblyStrategy());

    f.complete();

    //  Take a look at the output tuples
    TupleEntryIterator output = sink.openForRead(CascadingUtil.get().getFlowProcess());
    System.out.println("Output tuples from flow:");
    while(output.hasNext()){
      System.out.println(output.next().getTuple());
    }
  }

}
