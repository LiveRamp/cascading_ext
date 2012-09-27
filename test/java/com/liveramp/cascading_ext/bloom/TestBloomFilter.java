package com.liveramp.cascading_ext.bloom;

import java.io.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.hash.Hash64;

public class TestBloomFilter extends BaseTestCase {
  public void testSetSanity() throws IOException {
    BloomFilter set = new BloomFilter(1000000, 4, Hash64.MURMUR_HASH64);
    byte[] arr1 = new byte[] {1, 2, 3, 4, 5, 6, 7};
    byte[] arr2 = new byte[] {11, 12, 5, -2};
    byte[] arr3 = new byte[] {3, 4, 5};
    set.add(new Key(arr1));
    set.add(new Key(arr2));

    for (byte i = 0; i < (byte) 125; i++ ) {
      set.add(new Key(new byte[] {i}));
    }

    assertTrue(set.membershipTest(new Key(arr1)));
    assertTrue(set.membershipTest(new Key(arr2)));

    for (byte i = 0; i < (byte) 125; i++ ) {
      assertTrue(set.membershipTest(new Key(new byte[] {i})));
    }

    // technically this could be an invalid statement, but the probability is
    // low and this is a sanity check
    assertFalse(set.membershipTest(new Key(arr3)));

    // now test that we can write and read from file just fine
    new File("/tmp/filter-test.bloomfilter").delete();
    DataOutputStream os = new DataOutputStream(new FileOutputStream("/tmp/filter-test.bloomfilter"));
    set.write(os);
    os.close();

    BloomFilter set2 = new BloomFilter();
    DataInputStream is = new DataInputStream(new FileInputStream("/tmp/filter-test.bloomfilter"));
    set2.readFields(is);

    assertTrue(set2.membershipTest(new Key(arr1)));
    assertTrue(set2.membershipTest(new Key(arr2)));

    for (byte i = 0; i < (byte) 125; i++ ) {
      assertTrue(set2.membershipTest(new Key(new byte[] {i})));
    }

    // technically this could be an invalid statement, but the probability is low and this is a sanity check
    assertFalse(set2.membershipTest(new Key(arr3)));

    set2.acceptAll();
    for (byte i = 0; i < (byte) 125; i++ ) {
      assertTrue(set2.membershipTest(new Key(new byte[] {i})));
    }
    assertTrue(set2.membershipTest(new Key(arr3)));
  }

  public void testFalsePositiveRate() {
    BloomFilter set = new BloomFilter(100, 3, Hash64.MURMUR_HASH64);
    for (byte i = 0; i < 20; i++ ) {
      set.add(new Key(new byte[] {i}));
    }

    double rate = set.getFalsePositiveRate();
    assertEquals(92, Math.round(rate * 1000)); // from http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html

    set = new BloomFilter(100, 2, Hash64.MURMUR_HASH64);
    for (byte i = 0; i < 50; i++ ) {
      set.add(new Key(new byte[] {i}));
    }

    rate = set.getFalsePositiveRate();
    assertEquals(400, Math.round(rate * 1000)); // from http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
  }
}
