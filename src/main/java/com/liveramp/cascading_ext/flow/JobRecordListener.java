package com.liveramp.cascading_ext.flow;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.mapred.RunningJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowStep;
import cascading.flow.FlowStepListener;
import cascading.stats.hadoop.HadoopStepStats;

import com.liveramp.cascading_ext.counters.Counters;
import com.liveramp.cascading_ext.jobs.JobUtil;
import com.liveramp.commons.state.LaunchedJob;
import com.liveramp.commons.state.TaskSummary;

public class JobRecordListener implements FlowStepListener {
  private static final Logger LOG = LoggerFactory.getLogger(JobRecordListener.class);

  private final boolean failOnCounterFetch;
  private final JobPersister persister;
  private final Map<String, TaskSummary> taskSummaries;

  public JobRecordListener(JobPersister persister,
                           boolean failOnCounterFetch) {
    this.persister = persister;
    this.failOnCounterFetch = failOnCounterFetch;
    this.taskSummaries = Maps.newHashMap();
  }

  @Override
  public void onStepStarting(FlowStep flowStep) {
    LOG.info("Step starting");

    try {

      HadoopStepStats hdStepStats = (HadoopStepStats)flowStep.getFlowStepStats();
      RunningJob job = hdStepStats.getRunningJob();

      persister.onRunning(new LaunchedJob(job.getID().toString(),
              job.getJobName(),
              job.getTrackingURL())
      );

    } catch (NullPointerException | IOException e) {
      //  no op
    }

  }

  @Override
  public void onStepStopping(FlowStep flowStep) {
    LOG.info("Step stopping");
    HadoopStepStats hdStepStats = (HadoopStepStats)flowStep.getFlowStepStats();
    recordStepData(hdStepStats);
  }

  private void recordStepData(HadoopStepStats hdStepStats) {

    String jobID = hdStepStats.getJobID();

    try {

      persister.onCounters(
          jobID,
          Counters.getCounterMap(hdStepStats).get(jobID)
      );

    } catch (Exception e) {
      LOG.error("Failed to capture stats for step!", e);
      if (failOnCounterFetch) {
        throw new RuntimeException("Failed fetching stats for step", e);
      }
    }

    recordTaskErrors(hdStepStats, jobID, false);
  }

  private void recordTaskErrors(HadoopStepStats hdStepStats, String jobID, boolean failed) {
    long start = System.currentTimeMillis();

    try {

      LOG.info("Fetching task summaries...");
      TaskSummary taskSummary = JobUtil.getSummary(
          hdStepStats.getJobClient(),
          hdStepStats.getRunningJob().getID(),
          failed);
      LOG.info("Task summary collection took " + (System.currentTimeMillis() - start) + " millis.");

      persister.onTaskInfo(jobID, taskSummary);
      LOG.info("Done saving task summaries");

      if (taskSummaries.containsKey(jobID)) {
        throw new RuntimeException("Failed fetching task summaries: duplicate task summary/job found.");
      }
      taskSummaries.put(jobID, taskSummary);

    } catch (Exception e) {
      LOG.error("Error fetching task summaries", e);
      // getJobID on occasion throws a null pointer exception, ignore it
    }
  }

  //returns null if no task summaries have been recorded
  public Map<String, TaskSummary> getTaskSummaries() {
    return taskSummaries;
  }

  @Override
  public void onStepRunning(FlowStep flowStep) {
    //  no-op
  }

  @Override
  public void onStepCompleted(FlowStep flowStep) {
    LOG.info("Step completed");
    HadoopStepStats hdStepStats = (HadoopStepStats)flowStep.getFlowStepStats();
    recordStepData(hdStepStats);
  }

  @Override
  public boolean onStepThrowable(FlowStep flowStep, Throwable throwable) {
    HadoopStepStats hdStepStats = (HadoopStepStats)flowStep.getFlowStepStats();
    recordTaskErrors(hdStepStats, hdStepStats.getJobID(), true);
    return false;
  }
}
