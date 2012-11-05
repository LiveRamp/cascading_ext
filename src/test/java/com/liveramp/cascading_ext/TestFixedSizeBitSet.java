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

package com.liveramp.cascading_ext;


import org.junit.Test;

import static org.junit.Assert.*;

public class TestFixedSizeBitSet extends BaseTestCase {

  @Test
  public void testGetSet() {
    FixedSizeBitSet s = new FixedSizeBitSet(123);
    for (int i = 0; i < 123; i++) {
      assertTrue(!s.get(i));
    }
    s.set(5);
    s.set(8);
    for (int i = 0; i < 123; i++) {
      boolean expectedVal = false;
      if (i == 5 || i == 8) {
        expectedVal = true;
      }
      assertTrue(s.get(i) == expectedVal);
    }
    s.set(100);
    s.unset(8);
    for (int i = 0; i < 123; i++) {
      boolean expectedVal = false;
      if (i == 5 || i == 100) {
        expectedVal = true;
      }
      assertTrue(s.get(i) == expectedVal);
    }

  }

  @Test
  public void testFill() {
    FixedSizeBitSet s = new FixedSizeBitSet(1099);

    s.fill();

    s.unset(100);

    for (int i = 0; i < 1099; i++) {
      boolean expectedVal = true;
      if (i == 100) {
        expectedVal = false;
      }
      assertTrue(s.get(i) == expectedVal);
    }
  }
}
