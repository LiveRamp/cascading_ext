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

import cascading.tuple.Tuple;

import java.util.Iterator;

/**
 * An iterator (that wraps an Iterator<Tuple>) that iterates over a single position in a stream of tuples.
 *
 * @param <T>
 */
public class SingleElementTupleIterator<T> implements Iterator<T> {

  private final Iterator<Tuple> iter;
  private final int value;

  public SingleElementTupleIterator(Iterator<Tuple> interior, int pos) {
    this.iter = interior;
    this.value = pos;
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public T next() {
    Tuple t = iter.next();
    return (T) t.getObject(value);
  }

  @Override
  public void remove() {
    iter.remove();
  }
}
