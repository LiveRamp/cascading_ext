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
 * Decorator that creates copies of tuples returned by the decorated class.
 */
public class CopyingIterator implements Iterator<Tuple> {
  private final Iterator<Tuple> innerIterator;

  public CopyingIterator(Iterator<Tuple> innerIterator) {
    this.innerIterator = innerIterator;
  }

  @Override
  public boolean hasNext() {
    return innerIterator.hasNext();
  }

  @Override
  public Tuple next() {
    return new Tuple(innerIterator.next());
  }

  @Override
  public void remove() {
    innerIterator.remove();
  }
}
