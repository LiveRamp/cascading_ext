package com.liveramp.cascading_ext.bloom;

import cascading.tap.Tap;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import cascading.util.Pair;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.FixedSizeBitSet;
import com.liveramp.cascading_ext.assembly.BloomAssembly;
import com.liveramp.cascading_ext.hash2.HashFunction;
import com.liveramp.cascading_ext.hash2.HashFunctionFactory;
import org.apache.commons.collections.MapIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BloomUtil {
  private static Logger LOG = Logger.getLogger(BloomUtil.class);

  public static Pair<Double, Integer> getOptimalFalsePositiveRateAndNumHashes(long numBloomBits, long numElems){
    double falsePositiveRate = getFalsePositiveRate(BloomConstants.MAX_BLOOM_FILTER_HASHES, numBloomBits, numElems);
    double newFalsePositiveRate;
    int numBloomHashes = 1;
    for (int i = BloomConstants.MAX_BLOOM_FILTER_HASHES - 1; i > 0; i-- ) {
      newFalsePositiveRate = getFalsePositiveRate(i, numBloomBits, numElems);
      // Break out if you see an increase in false positive rate while decreasing the number of hashes.
      // Since this function has only one critical point, we know that if we see an increase
      // then the one we saw first was the minimum.
      // If we never see an increase then we know that one hash has the lowest false positive rate.
      if (falsePositiveRate > newFalsePositiveRate) {
        falsePositiveRate = newFalsePositiveRate;
      } else {
        numBloomHashes = i + 1;
        break;
      }
    }
    return new Pair<Double, Integer>(falsePositiveRate, numBloomHashes);
  }

  public static double getFalsePositiveRate(int numHashes, long vectorSize, long numElements) {
    // http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
    // Math.pow(1 - Math.pow(1-1/(double)m, k*n), k);
    return Math.pow(1.0 - Math.exp((double) -numHashes * numElements / vectorSize), numHashes);
  }

  public static BloomFilter mergeBloomParts(Tap tap, long numBloomBits, long splitSize, int numBloomHashes, long numElems, HashFunctionFactory factory) throws IOException {
    FixedSizeBitSet bitSet = new FixedSizeBitSet(numBloomBits);
    TupleEntryIterator itr = tap.openForRead(CascadingUtil.get().getFlowProcess());
    while (itr.hasNext()) {
      TupleEntry cur = itr.next();
      long split = cur.getLong(0);
      FixedSizeBitSet curSet = new FixedSizeBitSet(splitSize, ((BytesWritable) cur.getObject(1)).getBytes());
      for (long i = 0; i < curSet.numBits(); i++ ) {
        if (curSet.get(i)) {
          bitSet.set(split * splitSize + i);
        }
      }
    }

    return new BloomFilter(numBloomBits, numBloomHashes, factory.getFunction(numBloomBits, numBloomHashes), bitSet.getRaw(), numElems);
  }

  public static void configureDistCacheForBloomFilter(Map<Object, Object> properties, String bloomFilterPath) {
    properties.putAll(getPropertiesForDistCache(bloomFilterPath));
  }

  public static Map<String, String> getPropertiesForDistCache(String bloomFilterPath){
    try {
      LOG.info("Writing bloom filter to filesystem: "+FileSystemHelper.getFS().getUri().resolve(new URI(bloomFilterPath)).toString());
      return Collections.singletonMap("mapred.cache.files", FileSystemHelper.getFS().getUri().resolve(new URI(bloomFilterPath)).toString());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static void configureJobConfForRelevance(int requiredFieldsSize, int matchKeySize, Map<Object, Object> properties) {
    properties.putAll(getPropertiesForRelevance(requiredFieldsSize, matchKeySize));
  }

  public static Map<String, String> getPropertiesForRelevance(int requiredFieldsSize, int matchKeySize){
    Map<String, String> properties = new HashMap<String, String>();
    double bestIOSortRecordPercent = 16.0 / (16.0 + 8 + requiredFieldsSize + matchKeySize);
    bestIOSortRecordPercent = Math.max(Math.round(bestIOSortRecordPercent * 100) / 100.0, 0.01);
    properties.put("io.sort.record.percent", Double.toString(bestIOSortRecordPercent));
    properties.put("io.sort.mb", Integer.toString(BloomConstants.BUFFER_SIZE));
    return properties;
  }

  public static long getSplitSize(long numBloomBits, int numSplits){
    return (numBloomBits + numSplits - 1) / numSplits;
  }
}
