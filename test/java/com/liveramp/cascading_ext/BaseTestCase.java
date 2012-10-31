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

import com.liveramp.cascading_ext.bloom.BloomProps;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;

public abstract class BaseTestCase {
  private String TEST_ROOT;
  protected static final FileSystem fs = FileSystemHelper.getFS();

  static {
    Logger.getRootLogger().setLevel(Level.ALL);
  }

  @Before
  public void baseSetUp() throws Exception {
    TEST_ROOT = "/tmp/cascading_ext_" + this.getClass().getSimpleName() + "_AUTOGEN";

    // set the default job polling interval to 10ms. this makes the tests run *much* faster.
    CascadingUtil.get().setDefaultProperty("cascading.flow.job.pollinginterval", 10);
    CascadingUtil.get().setDefaultProperty("io.sort.record.percent", 0.10);

    CascadingUtil.get().setDefaultProperty(BloomProps.BUFFER_SIZE, 1);
    CascadingUtil.get().setDefaultProperty(BloomProps.NUM_BLOOM_BITS, 10);
    CascadingUtil.get().setDefaultProperty(BloomProps.BUFFER_SIZE, 5);

    fs.delete(new Path(TEST_ROOT), true);
    System.err.println("------ test start ------");
    System.out.println("------ test start ------");
  }

  protected String getTestRoot() {
    return TEST_ROOT;
  }
}
