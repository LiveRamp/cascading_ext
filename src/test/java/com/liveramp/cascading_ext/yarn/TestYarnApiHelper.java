package com.liveramp.cascading_ext.yarn;

import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestYarnApiHelper {
  private static final String RM1 = "rm149696";
  private static final String RM2 = "rm149698";

  private static final String ADDRESS1 = "address1";
  private static final String ADDRESS2 = "address2";

  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
  }

  @Test
  public void testGetYarnApiAddressMultipleRMs() {
    conf.set("yarn.resourcemanager.ha.rm-ids", RM1 + "," + RM2);
    conf.set("yarn.resourcemanager.webapp.address." + RM1, ADDRESS1);
    conf.set("yarn.resourcemanager.webapp.address." + RM2, ADDRESS2);

    Set<String> yarnApiAddresses = YarnApiHelper.getYarnApiAddresses(conf);

    assertEquals(Sets.newHashSet(ADDRESS1, ADDRESS2), yarnApiAddresses);
  }

  @Test
  public void testGetYarnApiAddressOneRM() {
    conf.set("yarn.resourcemanager.webapp.address", ADDRESS1);

    Set<String> yarnApiAddresses = YarnApiHelper.getYarnApiAddresses(conf);
    assertEquals(Sets.newHashSet(ADDRESS1), yarnApiAddresses);
  }

  @Test
  public void testGetYarnApiAddressNoRMs() {
    Set<String> yarnApiAddresses = YarnApiHelper.getYarnApiAddresses(conf);
    assertEquals(Sets.newHashSet(), yarnApiAddresses);
  }
}
