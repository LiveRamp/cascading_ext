package com.liveramp.cascading_ext.bloom_join;

import cascading.tap.Tap;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import cascading.util.Pair;
import com.liveramp.cascading_ext.FixedSizeBitSet;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BloomUtil {

  public static Pair<Double, Integer> getOptimalFalsePositiveRateAndNumHashes(long numBloomBits, long numElems){
    double falsePositiveRate = BloomFilter.falsePositiveRate(BloomConstants.MAX_BLOOM_FILTER_HASHES, numBloomBits, numElems);
    double newFalsePositiveRate;
    int numBloomHashes = 1;
    for (int i = BloomConstants.MAX_BLOOM_FILTER_HASHES - 1; i > 0; i-- ) {
      newFalsePositiveRate = BloomFilter.falsePositiveRate(i, numBloomBits, numElems);
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

  public synchronized static BytesBloomFilter mergeBloomParts(Tap tap, long numBloomBits, long splitSize, int numBloomHashes) throws IOException {
    FixedSizeBitSet bitSet = new FixedSizeBitSet(numBloomBits);
    TupleEntryIterator itr = tap.openForRead(CascadingHelper.getFlowProcess());
    while (itr.hasNext()) {
      TupleEntry cur = itr.next();
      long split = cur.getLong(0);
      FixedSizeBitSet curSet = new FixedSizeBitSet(splitSize, ((RapleafBytesWritable) cur.getObject(1)).getWithPadding());
      for (long i = 0; i < curSet.numBits(); i++ ) {
        if (curSet.get(i)) {
          bitSet.set(split * splitSize + i);
        }
      }
    }

    return new BytesBloomFilter(numBloomBits, numBloomHashes, bitSet.getRaw());
  }

  public static void configureDistCacheForBloomFilter(Map properties, String bloomFilterPath) {
    properties.putAll(getPropertiesForDistCache(bloomFilterPath));
  }

  public static Map<String, String> getPropertiesForDistCache(String bloomFilterPath){
    return Collections.singletonMap("mapred.cache.files", "hdfs://" + bloomFilterPath);
  }

  public static void configureJobConfForRelevance(int requiredFieldsSize, int matchKeySize, Map properties) {
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
