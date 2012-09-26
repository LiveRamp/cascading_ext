package com.liveramp.cascading_ext.bloom_join;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import com.liveramp.cascading_ext.FixedSizeBitSet;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class CreateBloomFilterFromIndices2 extends BaseOperation implements Aggregator {
  private long numBits;
  private final Tap sideBucket;
  private TupleEntryCollector collector;

  public CreateBloomFilterFromIndices2(Tap sideBucket) {
    super(Fields.NONE);
    this.sideBucket = sideBucket;
  }

  public static class Context {
    FixedSizeBitSet bitSet;
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall operationCall){
    try {
      collector = sideBucket.openForWrite(flowProcess);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void aggregate(FlowProcess flow, AggregatorCall call) {
    Context c = (Context) call.getContext();
    c.bitSet.set(call.getArguments().getLong(0));
  }

  @Override
  public void complete(FlowProcess flow, AggregatorCall call) {
    Context c = (Context) call.getContext();
    TupleEntry group = call.getGroup();
    collector.add(new Tuple(group.getObject("split"), new RapleafBytesWritable(c.bitSet.getRaw())));
  }

  @Override
  public void cleanup(FlowProcess flowProcess, OperationCall operationCall){
    collector.close();
  }

  @Override
  public void start(FlowProcess flow, AggregatorCall call) {
    JobConf conf = (JobConf) flow.getConfigCopy();
    numBits = Long.parseLong(conf.get("split.size"));

    Context c = new Context();
    c.bitSet = new FixedSizeBitSet(numBits, new byte[FixedSizeBitSet.getNumBytesToStore(numBits)]);
    call.setContext(c);
  }
}
