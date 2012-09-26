package com.liveramp.cascading_ext.bloom.operation;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.liveramp.cascading_ext.Bytes;
import com.liveramp.cascading_ext.TupleSerializationUtil;
import com.liveramp.cascading_ext.bloom.BloomFilterLoader;
import com.liveramp.cascading_ext.bloom.BloomFilterOperation;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class BloomJoinFilter extends BloomFilterOperation implements Filter {
  private transient TupleSerializationUtil tupleSerializationUtil;

  public BloomJoinFilter(BloomFilterLoader loader, String job_id, boolean cleanUpFilter) {
    super(loader, job_id, cleanUpFilter, Fields.ALL);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    tupleSerializationUtil = new TupleSerializationUtil((JobConf) flowProcess.getConfigCopy());
  }

  @Override
  public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
    ensureLoadedFilter(flowProcess);

    Tuple key = filterCall.getArguments().getTuple();
    try {
      byte[] serialized = tupleSerializationUtil.serialize(key);
      // filter it out (true) if the filter cannot contain it
      return !filterMayContain(Bytes.getBytes(new BytesWritable(serialized)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }
}
