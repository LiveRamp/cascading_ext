package com.liveramp.cascading_ext.operation.forwarding;

import cascading.flow.FlowProcess;
import cascading.operation.Operation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;

// A ForwardingOperation wraps an Operation instance and implements the same
// interface by forwarding calls to the underlying object.
// The point of this class is to provide common functionality to the specific
// Forwarding classes ForwardingFilter, ForwardingFunction, etc.
public class ForwardingOperation implements java.io.Serializable, cascading.operation.Operation {
  private final Operation operation;
  
  public ForwardingOperation(Operation operation) {
    this.operation = operation;
  }
  
  @Override
  public void cleanup(FlowProcess process, OperationCall call) {
    operation.cleanup(process, call);
  }
  
  @Override
  public Fields getFieldDeclaration() {
    return operation.getFieldDeclaration();
  }
  
  @Override
  public int getNumArgs() {
    return operation.getNumArgs();
  }
  
  @Override
  public boolean isSafe() {
    return operation.isSafe();
  }
  
  @Override
  public void prepare(FlowProcess process, OperationCall call) {
    operation.prepare(process, call);
  }

  @Override
  public void flush(FlowProcess flowProcess, OperationCall operationCall) {
    operation.flush(flowProcess, operationCall);
  }
}
