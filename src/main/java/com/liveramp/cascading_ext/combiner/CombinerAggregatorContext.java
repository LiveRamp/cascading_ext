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

package com.liveramp.cascading_ext.combiner;

import cascading.operation.AggregatorCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.ArrayList;

public class CombinerAggregatorContext<T> implements Serializable {

  private final CombinerDefinition<T> definition;

  private T aggregate;
  private TupleEntry key;
  private TupleEntry input;
  private ArrayList<Integer> keyFieldsPos;
  private ArrayList<Integer> inputFieldsPos;

  public CombinerAggregatorContext(CombinerDefinition<T> definition) {
    this.definition = definition;
  }

  public void start() {
    aggregate = definition.getFinalAggregator().initialize();
    key = new TupleEntry(definition.getGroupFields(), Tuple.size(definition.getGroupFields().size()));
    input = new TupleEntry(definition.getIntermediateFields(), Tuple.size(definition.getIntermediateFields().size()));
    keyFieldsPos = Lists.newArrayList();
    inputFieldsPos = Lists.newArrayList();
  }

  public void setGroupFields(AggregatorCall call) {
    CombinerUtils.setTupleEntry(key, keyFieldsPos, definition.getGroupFields(), call.getGroup());
  }

  public void setInputFields(AggregatorCall call) {
    CombinerUtils.setTupleEntry(input, inputFieldsPos, definition.getIntermediateFields(), call.getArguments());
  }

  public void aggregate() {
    aggregate = definition.getFinalAggregator().finalAggregate(aggregate, input);
  }

  public Tuple getAggregateTuple() {
    return key.getTuple().append(definition.getFinalAggregator().toFinalTuple(aggregate));
  }

  public CombinerDefinition<T> getDefinition() {
    return definition;
  }
}
