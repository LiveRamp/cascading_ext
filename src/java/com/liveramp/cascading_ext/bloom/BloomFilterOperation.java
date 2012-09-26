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

  private static Object _filter = null;
  private static String _filter_job_id = null;
  private final BloomFilterLoader loader;

  // the job id guarantees this stuff works on both cluster and during tests
  // in tests, the static objects don't get cleared between jobs
  private String _job_id;
  private boolean _cleanUpFilter;

  public BloomFilterOperation(BloomFilterLoader loader,
                              String job_id, boolean cleanUpFilter, Fields newFields) {
    super(newFields);

    _job_id = job_id;
    _cleanUpFilter = cleanUpFilter;

    this.loader = loader;
  }

  public BloomFilterOperation(BloomFilterLoader logic, String job_id, Fields newFields) {
    this(logic, job_id, true, newFields);
  }

  protected boolean filterMayContain(Object potential) {
    return loader.mayContain(_filter, potential);
  }

  protected void ensureLoadedFilter(FlowProcess process) {
    if (_filter == null || !_filter_job_id.equals(_job_id)) {
      try {
        LOG.info("Loading bloom filter");

        Path[] files = DistributedCache.getLocalCacheFiles((JobConf) process.getConfigCopy());
        List<Path> bloomFilterFiles = new ArrayList<Path>();
        LOG.info("cached files: ");
        for (Path p : files) {
          if (p.toString().endsWith(".bloomfilter")) {
            bloomFilterFiles.add(p);
          }
          LOG.info("file: " + p.toString());
        }
        if (bloomFilterFiles.size() != 1) {
          throw new RuntimeException("Expected one bloom filter path in the Distributed cache: there were " + bloomFilterFiles.size());
        }
        _filter = loader.loadFilter(FileSystem.getLocal(new Configuration()), bloomFilterFiles.get(0).toString());
        _filter_job_id = _job_id;

        LOG.info("Done loading bloom filter");
      } catch (IOException ioe) {
        throw new RuntimeException("Error loading bloom filter", ioe);
      }
    }
  }

  @Override
  public void cleanup(FlowProcess process, OperationCall call) {
    if (_cleanUpFilter) {
      try {
        List<Path> bloomFilterFiles = new ArrayList<Path>();
        Path[] files = DistributedCache.getLocalCacheFiles(((HadoopFlowProcess) process).getJobConf());
        for (Path p : files) {
          if (p.toString().endsWith(".bloomfilter")) {
            bloomFilterFiles.add(p);
          }
        }
        if (bloomFilterFiles.size() != 1) {
          throw new RuntimeException("Expected one bloom filter path in the Distributed cache: there were " + bloomFilterFiles.size());
        }
        FileSystemHelper.getFS().delete(new Path(bloomFilterFiles.get(0).toString()), true);
      } catch (IOException e) {
        LOG.info("Could not delete bloom filter " + ((HadoopFlowProcess) process).getJobConf().get("dustin.relevance.file"));
      }
    }
  }
}
