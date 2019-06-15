package com.liveramp.cascading_tools.jobs;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.stats.hadoop.HadoopStepStats;

import com.liveramp.cascading_ext.counters.Counters;
import com.liveramp.cascading_ext.flow.JobPersister;
import com.liveramp.cascading_ext.jobs.JobUtil;
import com.liveramp.cascading_ext.yarn.YarnApiHelper;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.commons.state.LaunchedJob;
import com.liveramp.commons.state.TaskSummary;

public class TrackedJob implements TrackedOperation {

  private static Logger LOG = LoggerFactory.getLogger(TrackedJob.class);

  private Runnable callback;
  private JobConf conf;

  public TrackedJob(JobConf conf) {
    this(conf, () -> {});
  }

  public TrackedJob(JobConf conf, Runnable callback) {
    this.conf = conf;
    this.callback = callback;
  }

  @Override
  public void complete(JobPersister persister, boolean failOnCounterFetch) {
    try {

      JobClient jobClient = new JobClient(conf);
      RunningJob job = jobClient.submitJob(conf);
      JobID jobId = job.getID();

      persister.onRunning(new LaunchedJob(jobId.toString(), job.getJobName(), job.getTrackingURL()));

      jobClient.monitorAndPrintJob(conf, job);

      logTaskSummaryInfo(persister, jobClient, jobId);
      if (job.isSuccessful()) {
        try {
          TwoNestedMap<String, String, Long> counterMap = Counters.getCounterMap(job);
          addYarnPerformanceMetrics(conf, jobId.getJtIdentifier(), counterMap);
          persister.onCounters(jobId.toString(), counterMap);
        } catch (IOException e) {
          if (failOnCounterFetch) {
            throw new RuntimeException(e);
          } else {
            LOG.error("Failed to fetch and persist counters: ", e);
          }
        }
      } else {
        throw new RuntimeException("Job failed! Job ID: " + jobId.toString() + " Job Name: " + job.getJobName());
      }

      callback.run();

    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void logTaskSummaryInfo(JobPersister persister, JobClient jobClient, JobID jobId) {
    try {
      TaskSummary taskSummary = JobUtil.getSummary(
          jobClient,
          jobId,
          true,
          new TwoNestedMap<String, String, Long>()
      );

      persister.onTaskInfo(jobId.toString(), taskSummary);
    } catch (InterruptedException | IOException e) {
      LOG.error("Error while retrieving summary of failed tasks", e);
    }
  }

  @Override
  public void stop() {

  }

  private void addYarnPerformanceMetrics(JobConf conf, String jobID, TwoNestedMap<String, String, Long> counters) {
    Optional<YarnApiHelper.ApplicationInfo> yarnAppInfo = Optional.empty();
    yarnAppInfo = YarnApiHelper.getYarnAppInfo(conf, jobID.replace("job", "application"));

    if (yarnAppInfo.isPresent()) {
      counters.putAll(yarnAppInfo.get().asCounterMap());
    }
  }
}
