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

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.fs.TrashHelper;
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
  // in production, we want to keep the static filter loaded for as long as possible
  //  (since jvm reuse might be enabled)
  // however, in tests, the static objects don't get cleared between jobs
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

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    if (filter == null || !filterJobId.equals(jobId)) {
      try {
        LOG.info("Loading bloom filter");

        String bloomFilter = getBloomFilterFile((JobConf) flowProcess.getConfigCopy());
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
        TrashHelper.deleteUsingTrashIfEnabled(FileSystemHelper.getFS(), new Path(bloomFilter));
      } catch (IOException e) {
        LOG.info("Error deleting bloom filter!");
      }
    }
  }

  private String getBloomFilterFile(JobConf config) {
    try {
      Path[] files = DistributedCache.getLocalCacheFiles(config);
      List<Path> bloomFilterFiles = new ArrayList<Path>();
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
