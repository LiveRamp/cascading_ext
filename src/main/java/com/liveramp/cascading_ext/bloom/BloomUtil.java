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

import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import cascading.util.Pair;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.liveramp.cascading_ext.Bytes;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.FixedSizeBitSet;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class BloomUtil {
  private static Logger LOG = Logger.getLogger(BloomUtil.class);

  private final static Object BF_LOAD_LOCK = new Object();

  public static Pair<Double, Integer> getOptimalFalsePositiveRateAndNumHashes(long numBloomBits, long numElems, int minHashes, int maxHashes) {
    if(maxHashes < minHashes){
      throw new IllegalArgumentException("Cannot have max # hashes smaller than min # hashes! "+maxHashes +" vs "+minHashes);
    }

    double bestFPRate = getFalsePositiveRate(maxHashes, numBloomBits, numElems);
    int bestBloomHashes = maxHashes;
    for (int i = maxHashes - 1; i >= minHashes; i--) {
      double newFalsePositiveRate = getFalsePositiveRate(i, numBloomBits, numElems);
      if(newFalsePositiveRate < bestFPRate){
        bestFPRate = newFalsePositiveRate;
        bestBloomHashes = i;
      }
    }
    return new Pair<Double, Integer>(bestFPRate, bestBloomHashes);
  }

  public static double getFalsePositiveRate(int numHashes, long vectorSize, long numElements) {
    // http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
    return Math.pow(1.0 - Math.exp((double) -numHashes * numElements / vectorSize), numHashes);
  }

  private static BloomFilter mergeBloomParts(String tapPath, long numBloomBits, long splitSize, int numBloomHashes, long numElems) throws IOException {
    FixedSizeBitSet bitSet = new FixedSizeBitSet(numBloomBits);

    if (FileSystemHelper.getFS().exists(new Path(tapPath))) {
      Hfs tap = new Hfs(new SequenceFile(new Fields("split", "filter")), tapPath);
      TupleEntryIterator itr = tap.openForRead(CascadingUtil.get().getFlowProcess());
      while (itr.hasNext()) {
        TupleEntry cur = itr.next();
        long split = cur.getLong(0);
        FixedSizeBitSet curSet = new FixedSizeBitSet(splitSize, ((BytesWritable) cur.getObject(1)).getBytes());
        for (long i = 0; i < curSet.numBits(); i++) {
          if (curSet.get(i)) {
            bitSet.set(split * splitSize + i);
          }
        }
      }
    }

    return new BloomFilter(numBloomBits, numBloomHashes, bitSet, numElems);
  }

  public static void configureDistCacheForBloomFilter(Map<Object, Object> properties, String bloomFilterPath) throws URISyntaxException {
    properties.putAll(getPropertiesForDistCache(bloomFilterPath));
  }

  public static Map<String, String> getPropertiesForDistCache(String bloomFilterPath) throws URISyntaxException {
    String path = FileSystemHelper.getFS().getUri().resolve(new URI(bloomFilterPath)).toString();
    return Collections.singletonMap(DistributedCache.CACHE_FILES, path);
  }

  public static void configureJobConfForBloomFilter(int requiredFieldsSize, int matchKeySize, Map<Object, Object> properties) {
    properties.putAll(getPropertiesForBloomFilter(requiredFieldsSize, matchKeySize));
  }

  public static Map<String, String> getPropertiesForBloomFilter(int requiredFieldsSize, int matchKeySize) {
    Map<String, String> properties = new HashMap<String, String>();
    double bestIOSortRecordPercent = 16.0 / (16.0 + 8 + requiredFieldsSize + matchKeySize);
    bestIOSortRecordPercent = Math.max(Math.round(bestIOSortRecordPercent * 100) / 100.0, 0.01);
    properties.put("io.sort.record.percent", Double.toString(bestIOSortRecordPercent));
    return properties;
  }

  public static long getSplitSize(long numBloomBits, int numSplits) {
    return (numBloomBits + numSplits - 1) / numSplits;
  }

  public static void writeFilterToHdfs(JobConf stepConf, String bloomTargetPath) throws IOException, CardinalityMergeException {
    String bloomPartsDir = stepConf.get(BloomProps.BLOOM_FILTER_PARTS_DIR);
    LOG.info("Bloom filter parts located in: " + bloomPartsDir);

    int maxHashes = BloomProps.getMaxBloomHashes(stepConf);
    int minHashes = BloomProps.getMinBloomHashes(stepConf);
    long bloomFilterBits = BloomProps.getNumBloomBits(stepConf);
    int numSplits = BloomProps.getNumSplits(stepConf);

    // This is the side bucket that the HyperLogLog writes to
    LOG.info("Getting key counts from: " + stepConf.get(BloomProps.BLOOM_KEYS_COUNTS_DIR));

    long prevJobTuples = getApproxDistinctKeysCount(stepConf, stepConf.get(BloomProps.BLOOM_KEYS_COUNTS_DIR));

    Pair<Double, Integer> optimal = getOptimalFalsePositiveRateAndNumHashes(bloomFilterBits, prevJobTuples, minHashes, maxHashes);
    LOG.info("Counted about " + prevJobTuples + " distinct keys");
    LOG.info("Using " + bloomFilterBits + " bits in the bloom filter");
    LOG.info("Found a false positive rate of: " + optimal.getLhs());
    LOG.info("Will use " + optimal.getRhs() + " bloom hashes");

    long splitSize = getSplitSize(bloomFilterBits, numSplits);
    int numBloomHashes = optimal.getRhs();

    synchronized (BF_LOAD_LOCK) {
      // Load bloom filter parts and merge them.
      String path = bloomPartsDir + "/" + numBloomHashes;
      BloomFilter filter = mergeBloomParts(path, bloomFilterBits, splitSize, numBloomHashes, prevJobTuples);

      // Write merged bloom filter to HDFS
      LOG.info("Writing created bloom filter to FS: " + bloomTargetPath);
      filter.writeOut(FileSystemHelper.getFS(), new Path(bloomTargetPath));
    }
  }

  /**
   * Read from the side bucket that HyperLogLog wrote to, merge the HLL estimators, and return the
   * approximate count of distinct keys
   */
  private static long getApproxDistinctKeysCount(JobConf conf, String partsDir) throws IOException, CardinalityMergeException {
    if (!FileSystemHelper.getFS().exists(new Path(partsDir))) {
      return 0;
    }

    Hfs approxCountsTap = new Hfs(new SequenceFile(new Fields("bytes")), partsDir);

    TupleEntryIterator in = approxCountsTap.openForRead(CascadingUtil.get().getFlowProcess());
    List<HyperLogLog> countParts = new LinkedList<HyperLogLog>();

    long totalSum = 0;
    while (in.hasNext()) {
      TupleEntry tuple = in.next();
      HyperLogLog card = HyperLogLog.Builder.build(Bytes.getBytes((BytesWritable) tuple.getObject("bytes")));
      countParts.add(card);
      totalSum += card.cardinality();
    }

    HyperLogLog merged = (HyperLogLog) new HyperLogLog(BloomProps.getHllErr(conf)).merge(countParts.toArray(new ICardinality[countParts.size()]));
    long cardinality = merged.cardinality();

    //  HLL estimation doesn't work over 2^32, and the cardinality code just returns 0.
    //  Honestly if you get this high, your bloom filter is probably saturated anyway, so just return that max.
    if (cardinality == 0 && totalSum != 0) {
      LOG.info("HyperLogLog likely reached its max estimation of 2^32! Returning that max, but true count likely higher.");
      return (long) Math.pow(2, 32);
    }

    return cardinality;
  }
}
