package com.liveramp.cascading_ext.bloom;

import com.liveramp.cascading_ext.BaseTestCase;

import java.io.*;

/**
 * @author eddie
 */
public class TestBytesBloomFilter extends BaseTestCase {
  public void testSetSanity() throws IOException {
    BytesBloomFilter set = new BytesBloomFilter(1000000, 4);
    byte[] arr1 = new byte[] {1, 2, 3, 4, 5, 6, 7};
    byte[] arr2 = new byte[] {11, 12, 5, -2};
    byte[] arr3 = new byte[] {3, 4, 5};
    set.add(arr1);
    set.add(arr2);

    for (byte i = 0; i < (byte) 125; i++ ) {
      set.add(new byte[] {i});
    }

    assertTrue(set.mayContain(arr1));
    assertTrue(set.mayContain(arr2));

    for (byte i = 0; i < (byte) 125; i++ ) {
      assertTrue(set.mayContain(new byte[] {i}));
    }

    // technically this could be an invalid statement, but the probability is low and this is a sanity check
    assertFalse(set.mayContain(arr3));

    // now test that we can write and read from file just fine
    new File("/tmp/filter-test.bloomfilter").delete();
    DataOutputStream os = new DataOutputStream(new FileOutputStream("/tmp/filter-test.bloomfilter"));
    set.write(os);
    os.close();

    BytesBloomFilter set2 = new BytesBloomFilter();
    DataInputStream is = new DataInputStream(new FileInputStream("/tmp/filter-test.bloomfilter"));
    set2.readFields(is);

    assertTrue(set2.mayContain(arr1));
    assertTrue(set2.mayContain(arr2));

    for (byte i = 0; i < (byte) 125; i++ ) {
      assertTrue(set2.mayContain(new byte[] {i}));
    }

    // technically this could be an invalid statement, but the probability is low and this is a sanity check
    assertFalse(set2.mayContain(arr3));

  }
}
