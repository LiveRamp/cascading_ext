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

import cascading.flow.hadoop.HadoopGroupByClosure;
import cascading.pipe.joiner.Joiner;
import cascading.pipe.joiner.JoinerClosure;
import cascading.tuple.Tuple;

import java.util.Iterator;

public class MultiGroupJoiner implements Joiner {
  protected MultiBuffer buffer;
  protected int groupSize;

  public MultiGroupJoiner(int groupSize, MultiBuffer buffer) {
    this.buffer = buffer;
    this.groupSize = groupSize;
  }

  @Override
  public Iterator<Tuple> getIterator(JoinerClosure joinerClosure) {

    buffer.setContext(groupSize + buffer.getResultFields().size(), (HadoopGroupByClosure) joinerClosure);
    buffer.operate();

    // See comment in CopyingIterator
    return new CopyingIterator(buffer.getResults().iterator());
  }

  public int numJoins() {
    return -1;
  }
}
