package com.liveramp.cascading_ext.bloom;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import com.liveramp.cascading_ext.FileSystemHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class BloomFilterOperation extends BaseOperation {
  private static Logger LOG = Logger.getLogger(BloomFilterOperation.class);

  private static BloomFilter filter = null;
  private static String filterJobId = null;

  // the job id guarantees this stuff works on both cluster and during tests
  // in tests, the static objects don't get cleared between jobs
  private String jobId;
  private boolean cleanUpFilter;

  public BloomFilterOperation(String jobId, boolean cleanUpFilter, Fields newFields) {
    super(newFields);

    this.jobId = jobId;
    this.cleanUpFilter = cleanUpFilter;
  }

  public BloomFilterOperation(String jobId, Fields newFields) {
    this(jobId, true, newFields);
  }

  protected boolean filterMayContain(byte[] potential) {
    return filter.membershipTest(potential);
  }

  protected void ensureLoadedFilter(FlowProcess process) {
    if (filter == null || !filterJobId.equals(jobId)) {
      JobConf conf = (JobConf) process.getConfigCopy();
      try {
        LOG.info("Loading bloom filter");

        String bloomFilter = getBloomFilterFile((JobConf) process.getConfigCopy());
        filter = BloomFilter.read(FileSystem.getLocal(new Configuration()),
            new Path(bloomFilter));
        filterJobId = jobId;

        LOG.info("Done loading bloom filter");
      } catch (Exception e) {
        throw new RuntimeException("Error loading bloom filter", e);
      }
    }
  }

  @Override
  public void cleanup(FlowProcess process, OperationCall call) {
    if (cleanUpFilter) {
      try {
        String bloomFilter = getBloomFilterFile((JobConf) process.getConfigCopy());
        FileSystemHelper.getFS().delete(new Path(bloomFilter), true);
      } catch (IOException e) {
        LOG.info("Could not delete bloom filter " + ((HadoopFlowProcess) process).getJobConf().get("dustin.relevance.file"));
      }
    }
  }

  private String getBloomFilterFile(JobConf config){
    try {
      Path[] files = DistributedCache.getLocalCacheFiles(config);
      List<Path> bloomFilterFiles = new ArrayList<Path>();
      LOG.info("cached files: ");
      for (Path p : files) {
        if (p.toString().endsWith(".bloomfilter")) {
          bloomFilterFiles.add(p);
        }
      }
      if (bloomFilterFiles.size() != 1) {
        throw new RuntimeException("Expected one bloom filter path in the Distributed cache: there were " + bloomFilterFiles.size());
      }
      return bloomFilterFiles.get(0).toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
