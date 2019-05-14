package com.liveramp.cascading_ext.combiner.lib;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;

public class TestHyperLogLogPlusBackwardsCompatibility {

  @Test
  public void testBackwardsCompatibility() throws IOException {
    // verifies that we can read HLL sketches generated previously and hardcoded as resources

    byte[] bytes = readHllBytes("hllp_small.bin");
    HyperLogLogPlus hll = HyperLogLogPlus.Builder.build(bytes);
    Assert.assertEquals(17815, hll.cardinality());

    byte[] bytes2 = readHllBytes("hllp_large.bin");
    HyperLogLogPlus hll2 = HyperLogLogPlus.Builder.build(bytes2);
    Assert.assertEquals(34144234, hll2.cardinality());
  }

  private byte[] readHllBytes(String resourceName) throws IOException {
    FileInputStream in = new FileInputStream(getClass().getClassLoader().getSystemResource(resourceName).getFile());
    return IOUtils.toByteArray(in);
  }
}
