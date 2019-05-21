package com.liveramp.cascading_ext.combiner.lib;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;


/**
 * This class tests that old sketches will always be serializable even if the version of stream-lib we depend on
 * is updated.
 */
public class TestHyperLogLogPlusBackwardsCompatibility {

  @Test
  public void testBackwardsCompatibility() throws IOException, CardinalityMergeException {
    // verifies that we can read HLL sketches generated previously and hardcoded as resources

    byte[] bytes = readHllBytes("hllp_small.bin");
    HyperLogLogPlus hll = HyperLogLogPlus.Builder.build(bytes);
    Assert.assertEquals(17815, hll.cardinality());

    byte[] bytes2 = readHllBytes("hllp_large.bin");
    HyperLogLogPlus hll2 = HyperLogLogPlus.Builder.build(bytes2);
    Assert.assertEquals(34144234, hll2.cardinality());

    Assert.assertEquals(34174200, hll.merge(hll2).cardinality());
  }

  private byte[] readHllBytes(String resourceName) throws IOException {
    FileInputStream in = new FileInputStream(getClass().getClassLoader().getSystemResource(resourceName).getFile());
    return IOUtils.toByteArray(in);
  }
}
