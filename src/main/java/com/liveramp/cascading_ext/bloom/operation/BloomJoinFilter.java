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

package com.liveramp.cascading_ext.bloom.operation;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.liveramp.cascading_ext.TupleSerializationUtil;
import com.liveramp.cascading_ext.bloom.BloomFilterOperation;

public class BloomJoinFilter extends BloomFilterOperation implements Filter {
  private transient TupleSerializationUtil tupleSerializationUtil;

  public BloomJoinFilter(String job_id, boolean cleanUpFilter) {
    super(job_id, cleanUpFilter, Fields.ALL);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    super.prepare(flowProcess, operationCall);
    tupleSerializationUtil = new TupleSerializationUtil((JobConf) flowProcess.getConfigCopy());
  }

  @Override
  public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
    Tuple key = filterCall.getArguments().getTuple();
    try {
      boolean result = !filterMayContain(tupleSerializationUtil.serialize(key));
      flowProcess.increment("BLOOM_JOIN_FILTERED_ITEM", Boolean.toString(result), 1);

      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}