package com.liveramp.cascading_ext.bloom.operation;

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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class CreateBloomFilterFromIndices extends BaseOperation implements Aggregator {
  private final Tap[] sideBuckets;
  private TupleEntryCollector[] collectors;

  public CreateBloomFilterFromIndices(Tap[] sideBuckets) {
    super(Fields.NONE);
    this.sideBuckets = sideBuckets;
  }

  public static class Context {
    FixedSizeBitSet[] bitSet;
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    try {
      collectors = new TupleEntryCollector[sideBuckets.length];
      for (int i = 0; i < sideBuckets.length; i++) {
        collectors[i] = sideBuckets[i].openForWrite(flowProcess);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void aggregate(FlowProcess flow, AggregatorCall call) {

    Context c = (Context) call.getContext();
    long bit = (Long) call.getArguments().getObject(0);
    int hashNum = (Integer) call.getArguments().getObject(1);

    for (int i = c.bitSet.length - 1; i > hashNum - 1; i--) {
      c.bitSet[i].set(bit);
    }
  }

  @Override
  public void complete(FlowProcess flow, AggregatorCall call) {
    Context c = (Context) call.getContext();
    TupleEntry group = call.getGroup();
    for (int i = 0; i < collectors.length; i++) {
      collectors[i].add(new Tuple(group.getObject("split"), new BytesWritable(c.bitSet[i].getRaw())));
    }
  }

  @Override
  public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
    for (TupleEntryCollector collector : collectors) {
      collector.close();
    }
  }

  @Override
  public void start(FlowProcess flow, AggregatorCall call) {
    JobConf conf = (JobConf) flow.getConfigCopy();
    long numBits = Long.parseLong(conf.get("split.size"));

    Context c = new Context();
    c.bitSet = new FixedSizeBitSet[sideBuckets.length];
    for (int i = 0; i < c.bitSet.length; i++) {
      c.bitSet[i] = new FixedSizeBitSet(numBits, new byte[FixedSizeBitSet.getNumBytesToStore(numBits)]);
    }
    call.setContext(c);
  }
}
