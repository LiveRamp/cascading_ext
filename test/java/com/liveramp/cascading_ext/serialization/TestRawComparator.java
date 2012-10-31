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

package com.liveramp.cascading_ext.serialization;

import com.liveramp.cascading_ext.BaseTestCase;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestRawComparator extends BaseTestCase {
  byte[] array1 = {1, 2, 3, 4};
  byte[] array2 = {2, 3, 4};
  byte[] array3 = {2, 3, 4};
  byte[] array4 = {2, 3, 4};
  byte[] array5 = {2, 3, 4};
  byte[] array6 = {1, 3, 4};

  @Test
  public void testOrdering() {
    assertOrder(array1, array2, -1);
    assertOrder(array3, array4, 0);
    assertOrder(array5, array6, 1);
  }

  private void assertOrder(byte[] array1, byte[] array2, int expected) {
    assertEquals(expected, RawComparator.compareByteArrays(array1, array2));
  }
}
