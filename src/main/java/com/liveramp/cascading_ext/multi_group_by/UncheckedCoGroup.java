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

package com.liveramp.cascading_ext.multi_group_by;

import cascading.flow.planner.Scope;
import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.Joiner;
import cascading.tuple.Fields;

public class UncheckedCoGroup extends CoGroup {

  public UncheckedCoGroup(Pipe[] pipes,
                          Fields[] groupFields,
                          Fields declaredFields,
                          Joiner joiner) {
    super(pipes, groupFields, declaredFields, joiner);
  }

  public Fields resolveFields(Scope scope) {
    return NaiveFields.fromFields(super.resolveFields(scope));
  }
}
