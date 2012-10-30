package com.liveramp.cascading_ext.operation.forwarding;

import cascading.flow.FlowProcess;
import cascading.operation.Operation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;

// A ForwardingOperation wraps an Operation instance and implements the same
// interface by forwarding calls to the underlying object.
// The point of this class is to provide common functionality to the specific
// Forwarding classes ForwardingFilter, ForwardingFunction, etc.
public class ForwardingOperation <C> implements java.io.Serializable, cascading.operation.Operation<C> {
  private final Operation<C> operation;

  public ForwardingOperation(Operation<C> operation) {
    this.operation = operation;
  }

  @Override
  public void cleanup(FlowProcess process, OperationCall<C> call) {
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
  public void prepare(FlowProcess process, OperationCall<C> call) {
    operation.prepare(process, call);
  }

  @Override
  public void flush(FlowProcess flowProcess, OperationCall<C> operationCall) {
    operation.flush(flowProcess, operationCall);
  }
}
