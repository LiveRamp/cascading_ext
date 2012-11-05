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

import org.apache.hadoop.mapred.JobConf;

import java.util.HashMap;
import java.util.Map;

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
  public static final String NUM_SPLITS = "cascading_ext.bloom.num.splits";
  public static final String BUFFER_SIZE = "cascading_ext.bloom.buffer.size";
  public static final String IO_SORT_PERCENT = "cascading_ext.bloom.io.sort.percent";

  /**
   * This parameter controls how accurate (and how much memory) HyperLogLog takes to approximate the
   * distinct number of keys
   */
  public static final String HLL_ERR = "cascading_ext.bloom.hll.error";
  /**
   * To compute optimal parameters for bloom filter creation, we need to know the average key size
   * and the average tuple size on the key side. This parameter controls the rate at which we sample
   * the keys to approximate an average.
   */
  public static final String KEY_SAMPLE_RATE = "cascading_ext.bloom.hll.sample.rate";

  //  default values for configurable params
  public static final long DEFAULT_NUM_BLOOM_BITS = 300L * 1024 * 1024 * 8;
  public static final int DEFAULT_MAX_BLOOM_FILTER_HASHES = 4;
  public static final int DEFAULT_BUFFER_SIZE = 300;
  public static final double DEFAULT_HLL_ERR = 0.01;
  public static double DEFAULT_KEY_SAMPLE_RATE = 0.01;
  public static double DEFAULT_IO_SORT_PERCENT = .5;

  public static Map<Object, Object> getDefaultProperties() {
    Map<Object, Object> properties = new HashMap<Object, Object>();
    properties.put(NUM_BLOOM_BITS, Long.toString(DEFAULT_NUM_BLOOM_BITS));
    properties.put(MAX_BLOOM_HASHES, Integer.toString(DEFAULT_MAX_BLOOM_FILTER_HASHES));
    properties.put(NUM_SPLITS, 100);
    properties.put(BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
    properties.put(HLL_ERR, DEFAULT_HLL_ERR);
    properties.put(KEY_SAMPLE_RATE, DEFAULT_KEY_SAMPLE_RATE);
    properties.put(IO_SORT_PERCENT, DEFAULT_IO_SORT_PERCENT);
    return properties;
  }

  public static long getNumBloomBits(JobConf conf) {
    return Long.parseLong(conf.get(NUM_BLOOM_BITS));
  }

  public static int getMaxBloomHashes(JobConf conf) {
    return Integer.parseInt(conf.get(MAX_BLOOM_HASHES));
  }

  public static int getNumSplits(JobConf conf) {
    return Integer.parseInt(conf.get(NUM_SPLITS));
  }

  public static int getBufferSize(JobConf conf) {
    return Integer.parseInt(conf.get(BUFFER_SIZE));
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

  public static double getKeySampleRate(JobConf conf) {
    return Double.parseDouble(conf.get(KEY_SAMPLE_RATE));
  }

  public static double getIOSortPercent(JobConf conf) {
    return Double.parseDouble(conf.get(IO_SORT_PERCENT));
  }
}
