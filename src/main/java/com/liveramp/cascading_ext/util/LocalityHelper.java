package com.liveramp.cascading_ext.util;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalityHelper {
  private static final int DEFAULT_MAX_BLOCK_LOCATIONS_PER_SPLIT = 3;

  public static String[] getHostsSortedByLocality(List<String> files, JobConf jobConf) throws IOException {
    return getHostsSortedByLocality(files, jobConf, DEFAULT_MAX_BLOCK_LOCATIONS_PER_SPLIT);
  }

  public static String[] getHostsSortedByLocality(List<String> files, JobConf jobConf, int maxBlockLocationsPerSplit) throws IOException {

    List<BlockLocation> allLocations = Lists.newArrayList();
    FileSystem fileSystem = FileSystem.get(jobConf);

    for (String file : files) {
      Path path = new Path(file);
      FileStatus status = fileSystem.getFileStatus(path);
      allLocations.addAll(Arrays.asList(fileSystem.getFileBlockLocations(status, 0, status.getLen())));
    }

    return getHostsSortedByLocalityForBlocks(allLocations, maxBlockLocationsPerSplit);
  }

  public static String[] getHostsSortedByLocality(String file, long pos, long length, JobConf jobConf, int maxBlockLocationsPerSplit) throws IOException {
    FileSystem fs = FileSystem.get(jobConf);
    BlockLocation[] locations = fs.getFileBlockLocations(fs.getFileStatus(new Path(file)), pos, length);
    return getHostsSortedByLocalityForBlocks(Arrays.asList(locations), maxBlockLocationsPerSplit);
  }

  public static String[] getHostsSortedByLocalityForBlocks(List<BlockLocation> blocks, int maxBlockLocationsPerSplit) throws IOException {

    Map<String, Long> numBytesPerHost = new HashMap<String, Long>();
    for (BlockLocation location : blocks) {
      Long size = location.getLength();
      for (String host : location.getHosts()) {
        incrNumBytesPerHost(numBytesPerHost, host, size);
      }
    }

    final int numHosts = Math.min(numBytesPerHost.size(), maxBlockLocationsPerSplit);

    List<ScoredHost> scoredHosts = new ArrayList<ScoredHost>(numBytesPerHost.size());
    for (Map.Entry<String, Long> entry : numBytesPerHost.entrySet()) {
      scoredHosts.add(new ScoredHost(entry.getKey(), entry.getValue()));
    }

    Collections.sort(scoredHosts);
    String[] sortedHosts = new String[numHosts];

    for (int i = 0; i < numHosts; i++) {
      sortedHosts[i] = scoredHosts.get(i).hostname;
    }

    return sortedHosts;
  }

  private static void incrNumBytesPerHost(Map<String, Long> numBytesPerHost, String host, Long numBytes) {
    if (!numBytesPerHost.containsKey(host)) {
      numBytesPerHost.put(host, numBytes);
    } else {
      numBytesPerHost.put(host, numBytesPerHost.get(host) + numBytes);
    }
  }

  private static class ScoredHost implements Comparable<ScoredHost> {
    public String hostname;
    public long numBytesInHost;

    public ScoredHost(String hostname, long numBytesInHost) {
      this.hostname = hostname;
      this.numBytesInHost = numBytesInHost;
    }

    @Override
    public int compareTo(ScoredHost scoredHost) {
      int bytesCmp = compareNumBytes(numBytesInHost, scoredHost.numBytesInHost);

      if (bytesCmp == 0) {
        return hostname.compareTo(scoredHost.hostname);
      }

      return bytesCmp;
    }

    // sort in reverse order by number of bytes in the host
    private static int compareNumBytes(long thisCount, long thatCount) {
      return Long.valueOf(thatCount).compareTo(thisCount);
    }
  }
}
