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
import cascading.operation.Filter;
import cascading.operation.FilterCall;

/**
 * A ForwardingFilter wraps a Filter instance and implements the same
 * interface by forwarding calls to the underlying object.
 * A Filter Decorator can be easily implemented by subclassing the
 * Forwarding class and overriding only the desired methods.
 * @param <C>
 */
public class ForwardingFilter <C> extends ForwardingOperation<C> implements Filter<C> {

  private final Filter<C> filter;

  public ForwardingFilter(Filter<C> filter) {
    super(filter);
    this.filter = filter;
  }

  @Override
  public boolean isRemove(FlowProcess process, FilterCall<C> call) {
    return filter.isRemove(process, call);
  }
}
