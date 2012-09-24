package com.liveramp.cascading_ext;

import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @author eddie
 */
public class BaseTestCase extends TestCase {
  static {
    Logger.getRootLogger().setLevel(Level.ALL);
  }

  public BaseTestCase(){
    // set the default job polling interval to 10ms. this makes the tests run *much* faster.
    CascadingUtil.get().setDefaultProperty("cascading.flow.job.pollinginterval", 10);
    CascadingUtil.get().setDefaultProperty("io.sort.mb", 1);
    CascadingUtil.get().setDefaultProperty("io.sort.record.percent", 0.10);
  }

  protected void setUp() throws Exception {
    super.setUp();
    System.err.println("------ test start ------");
    System.out.println("------ test start ------");
  }
}
