package com.liveramp.cascading_ext.multi_group_by;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;

public class MultiBufferOperation extends BaseOperation implements Buffer {

  private final MultiBuffer internalBuffer;

  public MultiBufferOperation(Fields keyFields, MultiBuffer multiBuffer) {
    super(keyFields.append(multiBuffer.getResultFields()));
    this.internalBuffer = multiBuffer;
  }

  @Override
  public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
    internalBuffer.setContext(bufferCall, flowProcess);
    internalBuffer.operate();
  }
}
