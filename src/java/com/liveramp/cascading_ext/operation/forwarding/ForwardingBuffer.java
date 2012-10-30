package com.liveramp.cascading_ext.operation.forwarding;

import cascading.flow.FlowProcess;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;

// A ForwardingBuffer wraps a Buffer instance and implements the same
// interface by forwarding calls to the underlying object.
// A Buffer Decorator can be easily implemented by subclassing the
// Forwarding class and overriding only the desired methods.
public class ForwardingBuffer <Context> extends ForwardingOperation<Context> implements Buffer<Context> {

  private final Buffer<Context> buffer;

  public ForwardingBuffer(Buffer<Context> buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  @Override
  public void operate(FlowProcess process, BufferCall<Context> call) {
    buffer.operate(process, call);
  }

}
