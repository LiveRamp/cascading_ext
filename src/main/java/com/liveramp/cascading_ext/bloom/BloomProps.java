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

package com.liveramp.cascading_ext.bloom;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;

public class BloomProps {

  //  dynamic, set at flow construction time
  public static final String SOURCE_BLOOM_FILTER_ID = "cascading_ext.bloom.source.filter.id";
  public static final String REQUIRED_BLOOM_FILTER_PATH = "cascading_ext.bloom.required.filter.path";
  public static final String TARGET_BLOOM_FILTER_ID = "cascading_ext.bloom.target.filter.id";
  public static final String BLOOM_KEYS_COUNTS_DIR = "cascading_ext.bloom.keys.counts.dir";
  public static final String BLOOM_FILTER_PARTS_DIR = "cascading_ext.bloom.target.filter.parts";

  //  configurable parameters
  public static final String NUM_BLOOM_BITS = "cascading_ext.bloom.num.bits";
  public static final String MAX_BLOOM_HASHES = "cascading_ext.bloom.max.hashes";
  public static final String MIN_BLOOM_HASHES = "cascading_ext.bloom.min.hashes";
  public static final String NUM_SPLITS = "cascading_ext.bloom.num.splits";

  public static final String TEST_MODE = "cascading_ext.bloom.test_mode";

  /**
   * This parameter controls how accurate (and how much memory) HyperLogLog takes to approximate the
   * distinct number of keys
   */
  public static final String HLL_ERR = "cascading_ext.bloom.hll.error";

  //  default values for configurable params
  public static final long DEFAULT_NUM_BLOOM_BITS = 300L * 1024 * 1024 * 8;
  public static final int DEFAULT_MAX_BLOOM_FILTER_HASHES = 4;
  public static final int DEFAULT_MIN_BLOOM_FILTER_HASHES = 1;
  public static final double DEFAULT_HLL_ERR = 0.01;

  private static final long TEST_MODE_NUM_BLOOM_BITS = 10;

  public static Map<Object, Object> getDefaultProperties() {
    Map<Object, Object> properties = new HashMap<Object, Object>();
    properties.put(NUM_BLOOM_BITS, Long.toString(DEFAULT_NUM_BLOOM_BITS));
    properties.put(MAX_BLOOM_HASHES, Integer.toString(DEFAULT_MAX_BLOOM_FILTER_HASHES));
    properties.put(MIN_BLOOM_HASHES, Integer.toString(DEFAULT_MIN_BLOOM_FILTER_HASHES));
    properties.put(NUM_SPLITS, 100);
    properties.put(HLL_ERR, DEFAULT_HLL_ERR);
    return properties;
  }

  public static long getNumBloomBits(JobConf conf) {

    if(isTest(conf)){
      return TEST_MODE_NUM_BLOOM_BITS;
    }

    return Long.parseLong(conf.get(NUM_BLOOM_BITS));
  }

  private static boolean isTest(JobConf conf) {
    return conf.getBoolean(TEST_MODE, false);
  }

  public static int getMaxBloomHashes(JobConf conf) {
    return Integer.parseInt(conf.get(MAX_BLOOM_HASHES));
  }

  public static int getMinBloomHashes(JobConf conf){
    return Integer.parseInt(conf.get(MIN_BLOOM_HASHES));
  }

  public static int getNumSplits(JobConf conf) {
    return Integer.parseInt(conf.get(NUM_SPLITS));
  }

  public static String getBloomFilterPartsDir(JobConf conf) {
    return conf.get(BLOOM_FILTER_PARTS_DIR);
  }

  public static String getApproxCountsDir(JobConf conf) {
    return conf.get(BLOOM_KEYS_COUNTS_DIR);
  }

  public static double getHllErr(JobConf conf) {
    return Double.parseDouble(conf.get(HLL_ERR));
  }

}
