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

package com.liveramp.cascading_ext.operation.forwarding;

import cascading.flow.FlowProcess;
import cascading.operation.Operation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;

/**
 * A ForwardingOperation wraps an Operation instance and implements the same
 * interface by forwarding calls to the underlying object.
 * The point of this class is to provide common functionality to the specific
 * Forwarding classes ForwardingFilter, ForwardingFunction, etc.
 *
 * @param <C>
 */
public class ForwardingOperation<C> implements java.io.Serializable, cascading.operation.Operation<C> {
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
