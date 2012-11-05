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
import com.liveramp.cascading_ext.BaseTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestSingleElementTupleIterator extends BaseTestCase {

  @Test
  public void testIt() {
    List<Tuple> tuples = new ArrayList<Tuple>();
    tuples.add(new Tuple("tuple", "number", "one"));
    tuples.add(new Tuple("tuple", "number", "two"));
    tuples.add(new Tuple("tuple", "number", "three"));

    SingleElementTupleIterator<String> it = new SingleElementTupleIterator<String>(tuples.iterator(), 2);
    assertTrue(it.hasNext());
    assertEquals("one", it.next());
    assertTrue(it.hasNext());
    assertEquals("two", it.next());
    assertTrue(it.hasNext());
    assertEquals("three", it.next());
    assertFalse(it.hasNext());
  }
}
