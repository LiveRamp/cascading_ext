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
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;

/**
 * A ForwardingAggregator wraps an Aggregator instance and implements the same
 * interface by forwarding calls to the underlying object.
 * An Aggregator Decorator can be easily implemented by subclassing the
 * Forwarding class and overriding only the desired methods.
 * @param <Context>
 */

public class ForwardingAggregator<Context> extends ForwardingOperation<Context> implements Aggregator<Context> {

  private final Aggregator<Context> aggregator;

  public ForwardingAggregator(Aggregator<Context> aggregator) {
    super(aggregator);
    this.aggregator = aggregator;
  }

  @Override
  public void start(FlowProcess process, AggregatorCall<Context> call) {
    aggregator.start(process, call);
  }

  @Override
  public void aggregate(FlowProcess process, AggregatorCall<Context> call) {
    aggregator.aggregate(process, call);
  }

  @Override
  public void complete(FlowProcess process, AggregatorCall<Context> call) {
    aggregator.complete(process, call);
  }
}
