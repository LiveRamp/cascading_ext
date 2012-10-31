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
import cascading.operation.Function;
import cascading.operation.FunctionCall;

/**
 * A ForwardingFunction wraps a Function instance and implements the same
 * interface by forwarding calls to the underlying object.
 * A Function Decorator can be easily implemented by subclassing the
 * Forwarding class and overriding only the desired methods.
 *
 * @param <Context>
 */
public class ForwardingFunction<Context> extends ForwardingOperation<Context> implements Function<Context> {

  private final Function<Context> function;

  public ForwardingFunction(Function<Context> function) {
    super(function);
    this.function = function;
  }

  @Override
  public void operate(FlowProcess process, FunctionCall<Context> call) {
    function.operate(process, call);
  }
}
