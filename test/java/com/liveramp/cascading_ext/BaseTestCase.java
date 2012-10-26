package com.liveramp.cascading_ext;

import com.liveramp.cascading_ext.bloom.BloomConstants;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

public abstract class BaseTestCase {
  private String TEST_ROOT;
  protected static final FileSystem fs = FileSystemHelper.getFS();

  static {
    Logger.getRootLogger().setLevel(Level.ALL);
  }

  @Before
  public void baseSetUp() throws Exception {
    TEST_ROOT = "/tmp/cascading_ext_"+ this.getClass().getSimpleName() + "_AUTOGEN";

    BloomConstants.DEFAULT_BLOOM_FILTER_BITS = 10;
    BloomConstants.BUFFER_SIZE = 10;

    // set the default job polling interval to 10ms. this makes the tests run *much* faster.
    CascadingUtil.get().setDefaultProperty("cascading.flow.job.pollinginterval", 10);
    CascadingUtil.get().setDefaultProperty("io.sort.mb", 1);
    CascadingUtil.get().setDefaultProperty("io.sort.record.percent", 0.10);

    fs.delete(new Path(TEST_ROOT), true);
    System.err.println("------ test start ------");
    System.out.println("------ test start ------");
  }

  protected String getTestRoot() {
    return TEST_ROOT;
  }
}
