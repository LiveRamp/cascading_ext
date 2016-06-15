package com.liveramp.cascading_ext.util;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;

import com.liveramp.commons.collections.CountingMap;

public class LocalityHelper {
  private static final Logger LOG = LoggerFactory.getLogger(LocalityHelper.class);
  private static final int DEFAULT_MAX_BLOCK_LOCATIONS_PER_SPLIT = 3;

  private static transient Map<String, String> hostToRack;
  private static transient Multimap<String, String> rackToHost;

  private synchronized static void loadTopology() {

    if (hostToRack == null) {
      try {

        hostToRack = Maps.newHashMap();
        rackToHost = HashMultimap.create();

        InputStream resource = LocalityHelper.class.getClassLoader().getResourceAsStream("topology.map");

        if (resource != null) {

          DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
          DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
          Document doc = dBuilder.parse(resource);

          NodeList nodes = doc.getElementsByTagName("node");

          for (int i = 0; i < nodes.getLength(); i++) {
            NamedNodeMap attributes = nodes.item(i).getAttributes();
            String name = attributes.getNamedItem("name").getTextContent();
            String rack = attributes.getNamedItem("rack").getTextContent();
            hostToRack.put(name, rack);
            rackToHost.put(rack, name);
          }

          LOG.info("Loaded topology map with " + hostToRack.size() + " hosts on "+rackToHost.keySet().size()+" racks");

        } else {
          LOG.warn("topology.map not found, using empty rack map (ignore if this is a test)");
        }

      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  public static Collection<String> getHostsOnRack(String rack){
    loadTopology();
    return rackToHost.get(rack);
  }

  public static String getRack(String host) {
    loadTopology();

    if(hostToRack.containsKey(host)){
      return hostToRack.get(host);
    }

    //  prevent NPE later, but also don't count this as as local with anything else
    LOG.error("Did not find rack for host: "+host);
    return host+"__RACK_NOT_FOUND";

  }

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

  public static Map<String, Long> getBytesPerHost(List<BlockLocation> blocks) throws IOException {
    CountingMap<String> numBytesPerHost = new CountingMap<>();
    for (BlockLocation location : blocks) {
      Long size = location.getLength();
      for (String host : location.getHosts()) {
        numBytesPerHost.increment(host, size);
      }
    }
    return numBytesPerHost.get();
  }

  public static String[] getHostsSortedByLocalityForBlocks(List<BlockLocation> blocks, int maxBlockLocationsPerSplit) throws IOException {
    return getBestNHosts(getBytesPerHost(blocks), maxBlockLocationsPerSplit);
  }

  public static String[] getBestNHosts(Map<String, Long> numBytesPerHost, int maxBlockLocationsPerSplit) {
    final int numHosts = Math.min(numBytesPerHost.size(), maxBlockLocationsPerSplit);

    List<ScoredLocation> scoredHosts = getScoredHosts(numBytesPerHost);
    String[] sortedHosts = new String[numHosts];

    for (int i = 0; i < numHosts; i++) {
      sortedHosts[i] = scoredHosts.get(i).location;
    }

    return sortedHosts;
  }

  private static List<ScoredLocation> getScoredHosts(Map<String, Long> numBytesPerLocation) {
    List<ScoredLocation> scoredHosts = new ArrayList<ScoredLocation>(numBytesPerLocation.size());
    for (Map.Entry<String, Long> entry : numBytesPerLocation.entrySet()) {
      scoredHosts.add(new ScoredLocation(entry.getKey(), entry.getValue()));
    }

    Collections.sort(scoredHosts);
    return scoredHosts;
  }

  public static Long getBytesOnBestHost(Map<String, Long> numBytesPerHost) {
    return getBytesInBestLocation(numBytesPerHost);
  }

  public static Long getBytesOnBestRack(Map<String, Long> numBytesPerHost) {
    CountingMap<String> numBytesPerRack = new CountingMap<>();
    for (Map.Entry<String, Long> entry : numBytesPerHost.entrySet()) {
      numBytesPerRack.increment(getRack(entry.getKey()), entry.getValue());
    }

    return getBytesInBestLocation(numBytesPerRack.get());
  }

  private static Long getBytesInBestLocation(Map<String, Long> numBytesPerLocation) {
    List<ScoredLocation> hosts = getScoredHosts(numBytesPerLocation);

    if (hosts.isEmpty()) {
      return 0L;
    }

    return hosts.get(0).numBytesInLocation;

  }


  private static class ScoredLocation implements Comparable<ScoredLocation> {
    public String location;
    public long numBytesInLocation;

    public ScoredLocation(String hostname, long numBytesInLocation) {
      this.location = hostname;
      this.numBytesInLocation = numBytesInLocation;
    }

    @Override
    public int compareTo(ScoredLocation scoredLocation) {
      int bytesCmp = compareNumBytes(numBytesInLocation, scoredLocation.numBytesInLocation);

      if (bytesCmp == 0) {
        return location.compareTo(scoredLocation.location);
      }

      return bytesCmp;
    }

    // sort in reverse order by number of bytes in the host
    private static int compareNumBytes(long thisCount, long thatCount) {
      return Long.valueOf(thatCount).compareTo(thisCount);
    }
  }
}
