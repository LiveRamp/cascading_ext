package com.liveramp.cascading_ext.operation.forwarding;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;

// A ForwardingFunction wraps a Function instance and implements the same
// interface by forwarding calls to the underlying object.
// A Function Decorator can be easily implemented by subclassing the
// Forwarding class and overriding only the desired methods.
public class ForwardingFunction extends ForwardingOperation implements Function {

  private final Function function;

  public ForwardingFunction(Function function) {
    super(function);
    this.function = function;
  }

  @Override
  public void operate(FlowProcess process, FunctionCall call) {
    function.operate(process, call);
  }
}
