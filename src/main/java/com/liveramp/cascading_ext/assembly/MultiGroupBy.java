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

package com.liveramp.cascading_ext.assembly;

import cascading.operation.*;
import cascading.pipe.*;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import com.liveramp.cascading_ext.multi_group_by.MultiBuffer;
import com.liveramp.cascading_ext.multi_group_by.MultiGroupJoiner;
import com.liveramp.cascading_ext.operation.AppendOperation;

import java.util.Arrays;

public class MultiGroupBy extends SubAssembly {

  public MultiGroupBy(Pipe p0, Pipe p1, Fields groupFields, MultiBuffer operation) {
    Pipe[] pipes = new Pipe[]{p0, p1};
    Fields[] fields = new Fields[]{groupFields, groupFields};
    init(pipes, findTailFields(pipes), fields, groupFields, operation);
  }

  public MultiGroupBy(Pipe p0, Fields group0, Pipe p1, Fields group1, Fields groupRename, MultiBuffer operation) {
    Pipe[] pipes = new Pipe[]{p0, p1};
    Fields[] fields = new Fields[]{group0, group1};
    init(pipes, findTailFields(pipes), fields, groupRename, operation);
  }

  public MultiGroupBy(Pipe[] pipes, Fields groupFields, MultiBuffer operation) {
    Fields[] allGroups = new Fields[pipes.length];
    Arrays.fill(allGroups, groupFields);
    init(pipes, findTailFields(pipes), allGroups, groupFields, operation);
  }

  public MultiGroupBy(Pipe[] pipes, Fields[] groupFields, Fields groupingRename, MultiBuffer operation) {
    init(pipes, findTailFields(pipes), groupFields, groupingRename, operation);
  }

  public MultiGroupBy(Pipe[] pipes, Fields[] allFields, Fields[] groupFields, Fields groupingRename, MultiBuffer operation) {
    init(pipes, allFields, groupFields, groupingRename, operation);
  }

  protected void init(Pipe[] pipes, Fields[] allFields, Fields[] groupFields, Fields groupingRename, MultiBuffer operation) {

    Fields groupTmp = makePad("__GROUP_TMP", groupingRename.size());
    Fields resultFields = Fields.join(groupTmp, operation.getResultFields());

    int inputsSum = 0;
    for(Fields field: allFields){
      inputsSum+=field.size();
    }

    //  rename pipe tails to be the same
    for(int i = 0; i < pipes.length; i++){
      pipes[i] = new RenameInPlace(pipes[i], allFields[i], groupFields[i], groupTmp);
    }

    //  pad output or input with more fields to make numbers add up
    int extraOutput = resultFields.size() - inputsSum;

    //  pad output with null fields
    if(extraOutput < 0){
      resultFields = resultFields.append(makePad("__MGB_TMP", -extraOutput));
    }

    //  need to pad one of the inputs with extra fields to allow the buffer to output the extra fields
    else if(extraOutput > 0){
      pipes[0] = new Each(pipes[0],
          new AppendOperation(makePad("__MGB_TMP", extraOutput)),
          Fields.ALL);
    }

    Fields[] groupCopy = new Fields[groupFields.length];
    Arrays.fill(groupCopy, groupTmp);

    Pipe result = new CoGroup(pipes, groupCopy, resultFields, new MultiGroupJoiner(inputsSum, operation));
    result = new Each(result, resultFields, new Identity());
    result = new RenameInPlace(result, resultFields, groupTmp, groupingRename);
    setTails(result);
  }

  private static Fields[] findTailFields(Pipe[] pipes){
    Fields[] tailFields = new Fields[pipes.length];
    for(int i = 0; i < pipes.length; i++){
      tailFields[i]= getDefinedOutputFields(pipes[i]);
    }
    return tailFields;
  }

  private static Fields makePad(String tmpName, int length){
    Fields groupTmp = new Fields();
    for(int i = 0; i < length; i++){
      groupTmp = groupTmp.append(new Fields(tmpName+i));
    }
    return groupTmp;
  }

  // return the pipe count for pipes we can identify.
  private static Fields getDefinedOutputFields(Pipe pipe){
    Fields outputFields = getAnyOutputFields(pipe);
    if(!outputFields.isDefined()){
      throw new RuntimeException("cannot find output arguments for pipe: "+pipe+"!");
    }
    return outputFields;
  }

  private static Fields getAnyOutputFields(Pipe pipe){
    while(true){

      //  try to get the output fields of a function, or if it's a filter
      //  skip to the previous pipe
      if(pipe instanceof Operator){
        Operator prev = (Operator) pipe;
        Fields outputSelector = prev.getOutputSelector();
        if(prev.getOperation() instanceof Filter){
          pipe = pipe.getPrevious()[0];
          continue;
        }
        return outputSelector;
      }

      //  if the cogroup declares its output, we can use it
      else if(pipe instanceof CoGroup){
        Splice prev = (Splice) pipe;
        return prev.getDeclaredFields();
      }

      //  retain operations we can easily find the output fields from
      else if(pipe instanceof Retain){
        Retain retain = (Retain) pipe;
        Each tail = (Each) retain.getTails()[0];
        return tail.getArgumentSelector();
      }

      //  if it's another kind of subassembly, just look inside it to see what the tail is
      else if(pipe instanceof SubAssembly){
        pipe = ((SubAssembly) pipe).getTails()[0];
      }

      //  renamed pipe, can just look further back
      else if(pipe.getClass() == Pipe.class){
        pipe = pipe.getPrevious()[0];
      }

      else{
        throw new RuntimeException("unable to get output fields size from pipe "+pipe+"!");
      }
    }
  }
}
