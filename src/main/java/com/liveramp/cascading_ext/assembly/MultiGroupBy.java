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

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import com.liveramp.cascading_ext.multi_group_by.MultiBuffer;
import com.liveramp.cascading_ext.multi_group_by.MultiGroupJoiner;
import com.liveramp.cascading_ext.multi_group_by.NaiveFields;
import com.liveramp.cascading_ext.multi_group_by.UncheckedCoGroup;

import java.util.Arrays;

public class MultiGroupBy extends SubAssembly {

  public MultiGroupBy(Pipe p0, Pipe p1, Fields groupFields, MultiBuffer operation) {
    Pipe[] pipes = new Pipe[]{p0, p1};
    Fields[] fields = new Fields[]{groupFields, groupFields};
    init(pipes, fields, groupFields, operation);
  }

  public MultiGroupBy(Pipe p0, Fields group0, Pipe p1, Fields group1, Fields groupRename, MultiBuffer operation) {
    Pipe[] pipes = new Pipe[]{p0, p1};
    Fields[] fields = new Fields[]{group0, group1};
    init(pipes, fields, groupRename, operation);
  }

  public MultiGroupBy(Pipe[] pipes, Fields groupFields, MultiBuffer operation) {
    Fields[] allGroups = new Fields[pipes.length];
    Arrays.fill(allGroups, groupFields);
    init(pipes, allGroups, groupFields, operation);
  }

  public MultiGroupBy(Pipe[] pipes, Fields[] groupFields, Fields groupingRename, MultiBuffer operation) {
    init(pipes, groupFields, groupingRename, operation);
  }

  protected void init(Pipe[] pipes, Fields[] groupFields, Fields groupingRename, MultiBuffer operation) {
    Fields resultFields = Fields.join(groupingRename, operation.getResultFields());
    Pipe result = new UncheckedCoGroup(pipes, groupFields, NaiveFields.fromFields(resultFields), new MultiGroupJoiner(groupingRename.size(), operation));
    result = new Each(result, resultFields, new Identity());
    setTails(result);
  }

}
