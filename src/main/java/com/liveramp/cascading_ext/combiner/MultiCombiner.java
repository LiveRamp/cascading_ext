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

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class MultiCombiner extends SubAssembly {

  public static final Comparable ID_FIELD = "__combiner_id";

  public MultiCombiner(Pipe[] pipes, List<CombinerDefinition> combinerDefinitions, boolean filterTails) {
    super(pipes);

    if (combinerIdsCollide(combinerDefinitions)) {
      throw new IllegalArgumentException("Some CombinerDefinition ids collide. " +
          "Make sure that all names are unique and, if they are, check for hashCode collisions");
    }

    Pipe[] pipesCopy = new Pipe[pipes.length];
    for (int i = 0; i < pipes.length; i++) {
      pipesCopy[i] = new Each(
          pipes[i],
          new MultiCombinerFunction(combinerDefinitions),
          Fields.RESULTS);
    }
    Pipe pipe = new GroupBy(pipesCopy, determineGroupFields(combinerDefinitions));
    pipe = new Every(pipe, new MultiCombinerAggregator(combinerDefinitions), Fields.RESULTS);

    if (filterTails) {
      Pipe[] tails = new Pipe[combinerDefinitions.size()];

      for (int i = 0; i < combinerDefinitions.size(); i++) {
        CombinerDefinition definition = combinerDefinitions.get(i);
        Pipe output = new Pipe(definition.getName() + "-intermediate", pipe);
        output = new Each(output, new Fields(ID_FIELD), new MultiCombinerFilter(definition.getId()));
        output = new Retain(output, definition.getGroupFields().append(definition.getOutputFields()));
        tails[i] = new Pipe(definition.getName(), output);
      }

      setTails(tails);
    } else {
      setTails(pipe);
    }
  }

  private boolean combinerIdsCollide(List<CombinerDefinition> combinerDefinitions) {
    Set<Integer> ids = Sets.newHashSet();
    for (CombinerDefinition definition : combinerDefinitions) {
      if (ids.contains(definition.getId())) {
        return true;
      }
      ids.add(definition.getId());
    }
    return false;
  }

  public static MultiCombiner assembly(Pipe pipe, CombinerDefinition... definitions) {
    return new MultiCombiner(new Pipe[]{pipe}, Lists.newArrayList(definitions), true);
  }

  public static MultiCombiner singleTailedAssembly(Pipe pipe, CombinerDefinition... definitions) {
    return new MultiCombiner(new Pipe[]{pipe}, Lists.newArrayList(definitions), false);
  }

  private static Fields determineGroupFields(List<CombinerDefinition> combinerDefinitions) {
    Fields summedGroupFields = new Fields(MultiCombiner.ID_FIELD);

    for (CombinerDefinition def : combinerDefinitions) {
      summedGroupFields = Fields.merge(summedGroupFields, def.getGroupFields());
    }
    return summedGroupFields;
  }

  public static Fields getOutputFields(List<CombinerDefinition> combinerDefinitions) {
    Fields summedOutputFields = new Fields(MultiCombiner.ID_FIELD);

    for (CombinerDefinition combinerDefinition : combinerDefinitions) {
      summedOutputFields =
          Fields.merge(
              summedOutputFields,
              combinerDefinition.getGroupFields(),
              combinerDefinition.getOutputFields()
          );
    }
    return summedOutputFields;
  }

  public static Fields getInputFields(List<CombinerDefinition> combinerDefinitions) {
    Fields summedInputFields = new Fields();

    for (CombinerDefinition combinerDefinition : combinerDefinitions) {
      summedInputFields =
          Fields.merge(
              summedInputFields,
              combinerDefinition.getGroupFields(),
              combinerDefinition.getInputFields()
          );
    }
    return summedInputFields;
  }

  public static Fields getIntermediateFields(List<CombinerDefinition> combinerDefinitions) {
    Fields summedIntermediateFields = new Fields(MultiCombiner.ID_FIELD);

    for (CombinerDefinition combinerDefinition : combinerDefinitions) {
      summedIntermediateFields =
          Fields.merge(
              summedIntermediateFields,
              combinerDefinition.getGroupFields(),
              combinerDefinition.getIntermediateFields()
          );
    }
    return summedIntermediateFields;
  }

  public static <T> void populateOutputTupleEntry(CombinerDefinition<T> definition, TupleEntry output, Tuple resultTuple) {
    //set the ID so we can differentiate later
    output.setRaw(MultiCombiner.ID_FIELD, definition.getId());

    //our tuples are of the form groupFields+outputFields, set the TupleEntry fields appropriately
    Fields groupFields = definition.getGroupFields();
    int index = 0;
    for (int i = 0; i < groupFields.size(); i++) {
      output.setRaw(groupFields.get(i), resultTuple.getObject(index));
      index++;
    }
    Fields outputFields = definition.getOutputFields();
    for (int i = 0; i < outputFields.size(); i++) {
      output.setRaw(outputFields.get(i), resultTuple.getObject(index));
      index++;
    }
  }

  public Map<String, Pipe> getTailsByName(){
    Map<String, Pipe> tails = Maps.newHashMap();
    for (Pipe pipe : getTails()) {
      tails.put(pipe.getName(), pipe);
    }
    return tails;
  }

  private static class MultiCombinerFilter extends BaseOperation implements Filter {

    private final Integer id;

    public MultiCombinerFilter(Integer id) {
      this.id = id;
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
      return !id.equals(filterCall.getArguments().getInteger(0));
    }
  }
}


