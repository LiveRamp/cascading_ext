package com.liveramp.cascading_ext.bloom;

import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
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
import com.liveramp.cascading_ext.assembly.CreateBloomFilter;
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

  public static Pair<Double, Integer> getOptimalFalsePositiveRateAndNumHashes(long numBloomBits, long numElems) {
    double falsePositiveRate = getFalsePositiveRate(BloomConstants.MAX_BLOOM_FILTER_HASHES, numBloomBits, numElems);
    double newFalsePositiveRate;
    int numBloomHashes = 1;
    for (int i = BloomConstants.MAX_BLOOM_FILTER_HASHES - 1; i > 0; i--) {
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
    return Math.pow(1.0 - Math.exp((double) -numHashes * numElements / vectorSize), numHashes);
  }

  public static BloomFilter mergeBloomParts(Tap tap, long numBloomBits, long splitSize, int numBloomHashes, long numElems) throws IOException {
    FixedSizeBitSet bitSet = new FixedSizeBitSet(numBloomBits);
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

    return new BloomFilter(numBloomBits, numBloomHashes, bitSet.getRaw(), numElems);
  }

  public static void configureDistCacheForBloomFilter(Map<Object, Object> properties, String bloomFilterPath) {
    properties.putAll(getPropertiesForDistCache(bloomFilterPath));
  }

  public static Map<String, String> getPropertiesForDistCache(String bloomFilterPath) {
    try {
      LOG.info("Writing bloom filter to filesystem: " + FileSystemHelper.getFS().getUri().resolve(new URI(bloomFilterPath)).toString());
      return Collections.singletonMap("mapred.cache.files", FileSystemHelper.getFS().getUri().resolve(new URI(bloomFilterPath)).toString());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static void configureJobConfForRelevance(int requiredFieldsSize, int matchKeySize, Map<Object, Object> properties) {
    properties.putAll(getPropertiesForRelevance(requiredFieldsSize, matchKeySize));
  }

  public static Map<String, String> getPropertiesForRelevance(int requiredFieldsSize, int matchKeySize) {
    Map<String, String> properties = new HashMap<String, String>();
    double bestIOSortRecordPercent = 16.0 / (16.0 + 8 + requiredFieldsSize + matchKeySize);
    bestIOSortRecordPercent = Math.max(Math.round(bestIOSortRecordPercent * 100) / 100.0, 0.01);
    properties.put("io.sort.record.percent", Double.toString(bestIOSortRecordPercent));
    properties.put("io.sort.mb", Integer.toString(BloomConstants.BUFFER_SIZE));
    return properties;
  }

  public static long getSplitSize(long numBloomBits, int numSplits) {
    return (numBloomBits + numSplits - 1) / numSplits;
  }

  public static void writeFilterToHdfs(JobConf stepConf, String requiredBloomPath) {
    String bloomPartsDir = stepConf.get("target.bloom.filter.parts");
    LOG.info("Bloom filter parts located in: " + bloomPartsDir);

    // This is the side bucket that the HyperLogLog writes to
    Tap approxCountsTap = new Hfs(new SequenceFile(new Fields("bytes")), stepConf.getRaw("bloom.keys.counts.dir"));

    long prevJobTuples = getApproxDistinctKeysCount(approxCountsTap);

    Pair<Double, Integer> optimal = BloomUtil.getOptimalFalsePositiveRateAndNumHashes(BloomConstants.DEFAULT_BLOOM_FILTER_BITS, prevJobTuples);
    LOG.info("Counted " + prevJobTuples + " distinct keys");
    LOG.info("Using " + BloomConstants.DEFAULT_BLOOM_FILTER_BITS + " bits in the bloom filter");
    LOG.info("Found a false positive rate of: " + optimal.getLhs());
    LOG.info("Will use " + optimal.getRhs() + " bloom hashes");

    long splitSize = Long.parseLong(stepConf.get("split.size"));
    int numBloomHashes = optimal.getRhs();

    try {
      synchronized (BF_LOAD_LOCK) {
        // Load bloom filter parts and merge them.
        Tap bloomParts = new Hfs(new SequenceFile(new Fields("split", "filter")), bloomPartsDir + "/" + (numBloomHashes - 1));
        BloomFilter filter = BloomUtil.mergeBloomParts(bloomParts, BloomConstants.DEFAULT_BLOOM_FILTER_BITS, splitSize, numBloomHashes,
            prevJobTuples);

        // Write merged bloom filter to HDFS
        LOG.info("Writing created bloom filter to FS: " + requiredBloomPath);
        filter.writeOut(FileSystemHelper.getFS(), new Path(requiredBloomPath));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Read from the side bucket that HyperLogLog wrote to, merge the HLL estimators, and return the
   * approximate count of distinct keys
   *
   * @param parts
   * @return
   */
  private static long getApproxDistinctKeysCount(Tap parts) {
    try {
      TupleEntryIterator in = parts.openForRead(CascadingUtil.get().getFlowProcess());
      List<HyperLogLog> countParts = new LinkedList<HyperLogLog>();

      while (in.hasNext()) {
        TupleEntry tuple = in.next();
        byte[] serializedHll = Bytes.getBytes((BytesWritable) tuple.getObject("bytes"));

        countParts.add(HyperLogLog.Builder.build(serializedHll));
      }

      ICardinality merged = new HyperLogLog(CreateBloomFilter.DEFAULT_ERR_PERCENTAGE).merge(countParts.toArray(new ICardinality[countParts.size()]));

      return merged.cardinality();
    } catch (IOException e) {
      throw new RuntimeException("couldn't open approximate distinct keys tap", e);
    } catch (CardinalityMergeException e) {
      throw new RuntimeException(e);
    }
  }

}
