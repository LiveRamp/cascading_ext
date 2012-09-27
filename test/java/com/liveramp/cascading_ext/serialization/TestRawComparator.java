package com.liveramp.cascading_ext.serialization;

import com.liveramp.cascading_ext.BaseTestCase;

public class TestRawComparator extends BaseTestCase {
  byte[] array1 = {1, 2, 3, 4};
  byte[] array2 = {2, 3, 4};
  byte[] array3 = {2, 3, 4};
  byte[] array4 = {2, 3, 4};
  byte[] array5 = {2, 3, 4};
  byte[] array6 = {1, 3, 4};

  public void testOrdering() {
    assertOrder(array1, array2, -1);
    assertOrder(array3, array4, 0);
    assertOrder(array5, array6, 1);
  }

  private void assertOrder(byte[] array1, byte[] array2, int expected){
    assertEquals(expected, RawComparator.compareByteArrays(array1, array2));
  }
}
