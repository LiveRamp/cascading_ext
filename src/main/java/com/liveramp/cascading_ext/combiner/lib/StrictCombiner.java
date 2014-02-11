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

package com.liveramp.cascading_ext.combiner.lib;

import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import com.liveramp.cascading_ext.combiner.Combiner;
import com.liveramp.cascading_ext.combiner.ExactAggregator;

public class StrictCombiner<T> extends Combiner<T> {

  public StrictCombiner(Pipe pipe,
                        ExactAggregator<T> aggregator,
                        Fields groupFields,
                        Fields inputFields,
                        Fields outputFields) {
    super(pipe, aggregator, groupFields, inputFields, outputFields, true);
  }

  public StrictCombiner(Pipe pipe,
                        ExactAggregator<T> aggregator,
                        Fields groupFields,
                        Fields inputFields,
                        Fields intermediateFields,
                        Fields outputFields) {
    super(pipe, aggregator, groupFields, inputFields, intermediateFields, outputFields, true);
  }

  public StrictCombiner(Pipe pipe,
                        ExactAggregator<T> aggregator,
                        Fields groupFields,
                        Fields inputFields,
                        Fields intermediateFields,
                        Fields outputFields,
                        int limit) {
    super(pipe, aggregator, groupFields, inputFields, intermediateFields, outputFields, limit, true);
  }

  public StrictCombiner(Pipe[] pipes,
                        ExactAggregator<T> aggregator,
                        Fields groupFields,
                        Fields inputFields,
                        Fields outputFields) {
    super(pipes, aggregator, groupFields, inputFields, outputFields, true);
  }

  public StrictCombiner(Pipe[] pipes,
                        ExactAggregator<T> aggregator,
                        Fields groupFields,
                        Fields inputFields,
                        Fields intermediateFields,
                        Fields outputFields) {
    super(pipes, aggregator, groupFields, inputFields, intermediateFields, outputFields, true);
  }

  public StrictCombiner(Pipe[] pipes,
                        ExactAggregator<T> aggregator,
                        Fields groupFields,
                        Fields inputFields,
                        Fields intermediateFields,
                        Fields outputFields,
                        int limit) {
    super(pipes, aggregator, groupFields, inputFields, intermediateFields, outputFields, limit, true);
  }

  public static <T> StrictCombiner<T> assembly(Pipe pipe,
                                               ExactAggregator<T> aggregator,
                                               Fields groupFields,
                                               Fields inputFields,
                                               Fields outputFields) {
    return new StrictCombiner<T>(pipe, aggregator, groupFields, inputFields, outputFields);
  }

  public static <T> StrictCombiner<T> assembly(Pipe pipe,
                                               ExactAggregator<T> aggregator,
                                               Fields groupFields,
                                               Fields inputFields,
                                               Fields intermediateFields,
                                               Fields outputFields) {
    return new StrictCombiner<T>(pipe, aggregator, groupFields, inputFields, intermediateFields, outputFields);
  }

  public static <T> StrictCombiner<T> assembly(Pipe pipe,
                                               ExactAggregator<T> aggregator,
                                               Fields groupFields,
                                               Fields inputFields,
                                               Fields intermediateFields,
                                               Fields outputFields,
                                               int limit) {
    return new StrictCombiner<T>(pipe, aggregator, groupFields, inputFields, intermediateFields, outputFields, limit);
  }

  public static <T> StrictCombiner<T> assembly(Pipe[] pipes,
                                               ExactAggregator<T> aggregator,
                                               Fields groupFields,
                                               Fields inputFields,
                                               Fields outputFields) {
    return new StrictCombiner<T>(pipes, aggregator, groupFields, inputFields, outputFields);
  }

  public static <T> StrictCombiner<T> assembly(Pipe[] pipes,
                                               ExactAggregator<T> aggregator,
                                               Fields groupFields,
                                               Fields inputFields,
                                               Fields intermediateFields,
                                               Fields outputFields) {
    return new StrictCombiner<T>(pipes, aggregator, groupFields, inputFields, intermediateFields, outputFields);
  }

  public static <T> StrictCombiner<T> assembly(Pipe[] pipes,
                                               ExactAggregator<T> aggregator,
                                               Fields groupFields,
                                               Fields inputFields,
                                               Fields intermediateFields,
                                               Fields outputFields,
                                               int limit) {
    return new StrictCombiner<T>(pipes, aggregator, groupFields, inputFields, intermediateFields, outputFields, limit);
  }
}
