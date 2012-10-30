package com.liveramp.cascading_ext.example;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepStrategy;
import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import com.liveramp.cascading_ext.CascadingUtil;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class shows how to extend CascadingUtil to attach default properties or flow step strategies which should be
 * applied to all jobs run on your cluster / cascading setup.
 */
public class FlowWithCustomCascadingUtil {

  /**
   * This class contains all the setup logic custom to your setup of hadoop / cascading.  Any properties or strategies
   * which you want to apply to every job can be set here for convenience.
   */
  public static class MyCascadingUtil extends CascadingUtil {
    private static final CascadingUtil INSTANCE = new MyCascadingUtil();
    public static CascadingUtil get() {
      return INSTANCE;
    }

    private MyCascadingUtil(){
      super();

      //  if the strategy doesn't require parameters, it can be added instead of a factory and instantiated
      //  with a newInstance call
      addDefaultFlowStepStrategy(MyFlowStepStrategy.class);

      //  set a default property.  This property can still be overridden either by properties passed into the flow
      //  connectory, attached to the pipe itself, or set a the flow step strategy
      setDefaultProperty("mapred.reduce.tasks", 5);

      //  calls to addSerialization or addSerializationToken should go here as well
    }
  }

  public static class MyFlowStepStrategy implements FlowStepStrategy<JobConf> {
    @Override
    public void apply(Flow<JobConf> jobConfFlow, List<FlowStep<JobConf>> flowSteps, FlowStep<JobConf> flowStep) {
      System.out.println("Applying a custom flow step strategy before running step!");
    }
  }

  public static void main(String[] args) throws IOException {
    if(args.length != 1){
      System.out.println("Usage: hadoop jar cascading_ext.job.jar com.liveramp.cascading_ext.example.FlowWithCustomCascadingUtil <output dir>");
      return;
    }

    String outputDir = args[0];
    Hfs sink = new Hfs(new SequenceFile(new Fields("field1", "field2", "field3", "field4")), outputDir);

    Pipe source1 = new Pipe("source1");

    Pipe source2 = new Pipe("source2");

    Pipe joined = new CoGroup(source1, new Fields("field1"),
        source2, new Fields("field3"));

    Map<String, Tap> sources = new HashMap<String, Tap>();
    sources.put("source1", ExampleFixtures.SOURCE_TAP_1);
    sources.put("source2", ExampleFixtures.SOURCE_TAP_2);

    //  if a cascading flow needs custom properties to be set, they can be set when passed into the flow connector
    Map<Object, Object> flowSpecificProps = new HashMap<Object, Object>();
    flowSpecificProps.put("io.sort.mb", "100");

    MyCascadingUtil.get().getFlowConnector(flowSpecificProps).connect("Example flow", sources, sink, joined).complete();

    //  Take a look at the output tuples
    TupleEntryIterator output = sink.openForRead(CascadingUtil.get().getFlowProcess());
    System.out.println("Output tuples from flow:");
    while(output.hasNext()){
      System.out.println(output.next().getTuple());
    }
  }
}
