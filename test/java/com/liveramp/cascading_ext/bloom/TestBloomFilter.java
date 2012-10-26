package com.liveramp.cascading_ext.bloom;

import com.liveramp.cascading_ext.BaseTestCase;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

public class TestBloomFilter extends BaseTestCase {

  @Test
  public void testSetSanity() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {

    BloomFilter set = new BloomFilter(1000000, 4);
    byte[] arr1 = new byte[]{1, 2, 3, 4, 5, 6, 7};
    byte[] arr2 = new byte[]{11, 12, 5, -2};
    byte[] arr3 = new byte[]{3, 4, 5};
    set.add(arr1);
    set.add(arr2);

    for (byte i = 0; i < (byte) 125; i++) {
      set.add(new byte[]{i});
    }

    assertTrue(set.membershipTest(arr1));
    assertTrue(set.membershipTest(arr2));

    for (byte i = 0; i < (byte) 125; i++) {
      assertTrue(set.membershipTest(new byte[]{i}));
    }

    // technically this could be an invalid statement, but the probability is
    // low and this is a sanity check
    assertFalse(set.membershipTest(arr3));

    // now test that we can write and read from file just fine
    new File("/tmp/filter-test.bloomfilter").delete();

    DataOutputStream os = new DataOutputStream(new FileOutputStream("/tmp/filter-test.bloomfilter"));
    set.write(os);
    os.close();

    BloomFilter set2 = new BloomFilter();
    DataInputStream in = new DataInputStream(new FileInputStream("/tmp/filter-test.bloomfilter"));
    set2.readFields(in);
    in.close();

    assertTrue(set2.membershipTest(arr1));
    assertTrue(set2.membershipTest(arr2));

    for (byte i = 0; i < (byte) 125; i++) {
      assertTrue(set2.membershipTest(new byte[]{i}));
    }

    // technically this could be an invalid statement, but the probability is low and this is a sanity check
    assertFalse(set2.membershipTest(arr3));

  }

  @Test
  public void testFalsePositiveRate() {
    BloomFilter set = new BloomFilter(100, 3);
    for (byte i = 0; i < 20; i++) {
      set.add(new byte[]{i});
    }

    double rate = set.getFalsePositiveRate();
    assertEquals(92, Math.round(rate * 1000)); // from http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html

    set = new BloomFilter(100, 2);
    for (byte i = 0; i < 50; i++) {
      set.add(new byte[]{i});
    }

    rate = set.getFalsePositiveRate();
    assertEquals(400, Math.round(rate * 1000)); // from http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
  }
}
