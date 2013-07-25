/**
 * Copyright 2012 LiveRamp
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.liveramp.cascading_ext.flow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskCompletionEvent.Status;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import cascading.flow.Flow;
import cascading.flow.FlowException;
import cascading.flow.FlowListener;
import cascading.flow.FlowProcess;
import cascading.flow.FlowSkipStrategy;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepStrategy;
import cascading.flow.planner.PlatformInfo;
import cascading.management.UnitOfWorkSpawnStrategy;
import cascading.stats.FlowStats;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopStepStats;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.liveramp.cascading_ext.counters.Counters;

/**
 * Delegates actual flow operations to a flow that gets passed in, but performs some additional logging when the job
 * completes or fails.
 */
public class LoggingFlow implements Flow<JobConf> {
  private static final Pattern LOG_ERROR_PATTERN = Pattern.compile("Caused by.*?\\d\\d\\d\\d-\\d\\d-\\d\\d", Pattern.DOTALL);
  private static Logger LOG = Logger.getLogger(Flow.class);
  private static final int FAILURES_TO_QUERY = 3;

  private final Flow<JobConf> internalFlow;

  public LoggingFlow(Flow<JobConf> internalFlow) {
    this.internalFlow = internalFlow;
  }

  @Override
  public void complete() {
    try {
      internalFlow.complete();
      logJobIDs();
      logCounters();
    } catch (FlowException e) {
      logJobIDs();
      String jobErrors = logJobErrors();
      throw new RuntimeException(jobErrors, e);
    }
  }

  @Override
  public void cleanup() {
    internalFlow.cleanup();
  }

  @Override
  public TupleEntryIterator openSource() throws IOException {
    return internalFlow.openSource();
  }

  @Override
  public TupleEntryIterator openSource(String name) throws IOException {
    return internalFlow.openSource(name);
  }

  @Override
  public TupleEntryIterator openSink() throws IOException {
    return internalFlow.openSink();
  }

  @Override
  public TupleEntryIterator openSink(String name) throws IOException {
    return internalFlow.openSink(name);
  }

  @Override
  public TupleEntryIterator openTrap() throws IOException {
    return internalFlow.openTrap();
  }

  @Override
  public TupleEntryIterator openTrap(String name) throws IOException {
    return internalFlow.openTrap(name);
  }

  private void logJobIDs() {
    boolean exceptions = false;
    try {
      List<FlowStepStats> stepStats = internalFlow.getFlowStats().getFlowStepStats();
      List<String> jobIDs = new ArrayList<String>();
      for (FlowStepStats stat : stepStats) {

        try {
          JobID jobid = ((HadoopStepStats)stat).getRunningJob().getID();
          String jtID = jobid.getJtIdentifier();
          String jobID = Integer.toString(jobid.getId());
          jobIDs.add(jtID + "_" + jobID);
        } catch (Exception e) {
          exceptions = true;
        }
      }

      if (exceptions) {
        LOG.info("unable to retrieve jobid from all completed steps!");
        LOG.info("successfully retrieved job ids: " + StringUtils.join(jobIDs, ", "));
      } else {
        LOG.info("step job ids: " + StringUtils.join(jobIDs, ", "));
      }
    } catch (Exception e) {
      LOG.info("unable to retrieve any jobids from steps");
    }
  }

  private String logJobErrors() {
    boolean exceptions = false;
    StringBuilder jobErrors = new StringBuilder();
    try {
      List<FlowStepStats> stepStats = internalFlow.getFlowStats().getFlowStepStats();
      Set<String> jobFailures = new HashSet<String>();
      for (FlowStepStats stat : stepStats) {
        try {
          RunningJob job = ((HadoopStepStats)stat).getRunningJob();
          TaskCompletionEvent[] events = job.getTaskCompletionEvents(0);
          ArrayList<TaskCompletionEvent> failures = new ArrayList<TaskCompletionEvent>();
          for (TaskCompletionEvent event : events) {
            if (event.getTaskStatus() == Status.FAILED) {
              failures.add(event);
            }
          }
          // We limit the number of potential logs being pulled to spare the jobtracker
          if (failures.size() > 0) {
            Collections.shuffle(failures);
            for (int i = 0; i < FAILURES_TO_QUERY; i++) {
              jobFailures.add(getFailureLog(failures.get(i)));
            }
          }
        } catch (Exception e) {
          exceptions = true;
        }
      }
      if (exceptions) {
        logAndAppend(jobErrors, "unable to retrieve failures from all completed steps!");
        logAndAppend(jobErrors, "successfully retrieved job failures: " + StringUtils.join(jobFailures, ", "));
      } else {
        logAndAppend(jobErrors, "step attempt failures: " + StringUtils.join(jobFailures, ", "));
      }
    } catch (Exception e) {
      logAndAppend(jobErrors, "unable to retrieve any failures from steps");
      logAndAppend(jobErrors, e.toString());
    }
    return jobErrors.toString();
  }

  private static void logAndAppend(StringBuilder sb, String str) {
    Log.info(str);
    sb.append(str);
  }

  private static String getFailureLog(TaskCompletionEvent event) {
    LOG.info("Getting errors for attempt " + event.getTaskAttemptId());
    String exception = "";
    try {
      String fullLog = retrieveTaskLogs(event.getTaskAttemptId(), event.getTaskTrackerHttp());
      exception = extractErrorFromLogString(fullLog);
    } catch (IOException e) {
      LOG.info("Regex Error!", e);
    }
    return "\nCluster Log Exception:\n" + exception;
  }

  protected static String extractErrorFromLogString(String fullLog) {
    String exception;
    Matcher matcher = LOG_ERROR_PATTERN.matcher(fullLog);
    matcher.find();
    exception = matcher.group();
    exception = exception.substring(0, exception.length() - 10);
    return exception;
  }

  // This method pulled wholesale from a private method in hadoop's JobClient
  private static String retrieveTaskLogs(TaskAttemptID taskId, String baseUrl)
      throws IOException {
    // The tasktracker for a 'failed/killed' job might not be around...
    if (baseUrl != null) {
      // Construct the url for the tasklogs
      String taskLogUrl = getTaskLogURL(taskId, baseUrl);

      // Copy tasks's stdout of the JobClient
      return getTaskLogStream(new URL(taskLogUrl + "&filter=syslog"));

    }
    return "";
  }

  static String getTaskLogURL(TaskAttemptID taskId, String baseUrl) {
    return (baseUrl + "/tasklog?plaintext=true&attemptid=" + taskId);
  }

  // This method pulled wholesale from a private method in hadoop's JobClient
  private static String getTaskLogStream(URL taskLogUrl) {
    try {
      URLConnection connection = taskLogUrl.openConnection();
      BufferedReader input =
          new BufferedReader(new InputStreamReader(connection.getInputStream()));
      try {
        StringBuilder logData = new StringBuilder();
        String line;
        while ((line = input.readLine()) != null) {
          logData.append(line).append("\n");
        }
        return logData.toString();
      } finally {
        input.close();
      }

    } catch (IOException ioe) {
      LOG.warn("Error reading task output" + ioe.getMessage());
      return "";
    }
  }

  private void logCounters() {
    LOG.info(Counters.prettyCountersString(this));
  }

  @Override
  public String getID() {
    return internalFlow == null ? "NA" : internalFlow.getID();
  }

  @Override
  public String getTags() {
    return internalFlow.getTags();
  }

  @Override
  public int getSubmitPriority() {
    return internalFlow.getSubmitPriority();
  }

  @Override
  public void setSubmitPriority(int submitPriority) {
    internalFlow.setSubmitPriority(submitPriority);
  }

  @Override
  public String getCascadeID() {
    return internalFlow.getCascadeID();
  }

  @Override
  public String getRunID() {
    return internalFlow.getRunID();
  }

  @Override
  public PlatformInfo getPlatformInfo() {
    return internalFlow.getPlatformInfo();
  }

  @Override
  public void setSpawnStrategy(UnitOfWorkSpawnStrategy spawnStrategy) {
    internalFlow.setSpawnStrategy(spawnStrategy);
  }

  @Override
  public UnitOfWorkSpawnStrategy getSpawnStrategy() {
    return internalFlow.getSpawnStrategy();
  }

  @Override
  public boolean stepsAreLocal() {
    return internalFlow.stepsAreLocal();
  }

  @Override
  public boolean resourceExists(Tap tap) throws IOException {
    return internalFlow.resourceExists(tap);
  }

  @Override
  public TupleEntryIterator openTapForRead(Tap tap) throws IOException {
    return internalFlow.openTapForRead(tap);
  }

  @Override
  public TupleEntryCollector openTapForWrite(Tap tap) throws IOException {
    return internalFlow.openTapForWrite(tap);
  }

  @Override
  public String toString() {
    return internalFlow.toString();
  }

  @Override
  public void writeDOT(String filename) {
    internalFlow.writeDOT(filename);
  }

  @Override
  public void writeStepsDOT(String filename) {
    internalFlow.writeStepsDOT(filename);
  }

  @Override
  public JobConf getConfig() {
    return internalFlow.getConfig();
  }

  @Override
  public JobConf getConfigCopy() {
    return internalFlow.getConfigCopy();
  }

  @Override
  public Map<Object, Object> getConfigAsProperties() {
    return internalFlow.getConfigAsProperties();
  }

  @Override
  public String getProperty(String key) {
    return internalFlow.getProperty(key);
  }

  @Override
  public FlowProcess<JobConf> getFlowProcess() {
    return internalFlow.getFlowProcess();
  }

  @Override
  public String getName() {
    return internalFlow.getName();
  }

  @Override
  public FlowStats getFlowStats() {
    return internalFlow.getFlowStats();
  }

  @Override
  public FlowStats getStats() {
    return internalFlow.getStats();
  }

  @Override
  public boolean hasListeners() {
    return internalFlow.hasListeners();
  }

  @Override
  public void addListener(FlowListener flowListener) {
    internalFlow.addListener(flowListener);
  }

  @Override
  public boolean removeListener(FlowListener flowListener) {
    return internalFlow.removeListener(flowListener);
  }

  @Override
  public Map<String, Tap> getSources() {
    return internalFlow.getSources();
  }

  @Override
  public Tap getSource(String name) {
    return internalFlow.getSource(name);
  }

  @Override
  public Collection<Tap> getSourcesCollection() {
    return internalFlow.getSourcesCollection();
  }

  @Override
  public Map<String, Tap> getSinks() {
    return internalFlow.getSinks();
  }

  @Override
  public Tap getSink(String name) {
    return internalFlow.getSink(name);
  }

  @Override
  public Collection<Tap> getSinksCollection() {
    return internalFlow.getSinksCollection();
  }

  @Override
  public Tap getSink() {
    return internalFlow.getSink();
  }

  @Override
  public Map<String, Tap> getTraps() {
    return internalFlow.getTraps();
  }

  @Override
  public Collection<Tap> getTrapsCollection() {
    return internalFlow.getTrapsCollection();
  }

  @Override
  public Map<String, Tap> getCheckpoints() {
    return internalFlow.getCheckpoints();
  }

  @Override
  public Collection<Tap> getCheckpointsCollection() {
    return internalFlow.getCheckpointsCollection();
  }

  @Override
  public boolean isStopJobsOnExit() {
    return internalFlow.isStopJobsOnExit();
  }

  @Override
  public FlowSkipStrategy getFlowSkipStrategy() {
    return internalFlow.getFlowSkipStrategy();
  }

  @Override
  public FlowSkipStrategy setFlowSkipStrategy(FlowSkipStrategy flowSkipStrategy) {
    return internalFlow.setFlowSkipStrategy(flowSkipStrategy);
  }

  @Override
  public boolean isSkipFlow() throws IOException {
    return internalFlow.isSkipFlow();
  }

  @Override
  public boolean areSinksStale() throws IOException {
    return internalFlow.areSinksStale();
  }

  @Override
  public boolean areSourcesNewer(long sinkModified) throws IOException {
    return internalFlow.areSourcesNewer(sinkModified);
  }

  @Override
  public long getSinkModified() throws IOException {
    return internalFlow.getSinkModified();
  }

  @Override
  public FlowStepStrategy getFlowStepStrategy() {
    return internalFlow.getFlowStepStrategy();
  }

  @Override
  public void setFlowStepStrategy(FlowStepStrategy flowStepStrategy) {
    internalFlow.setFlowStepStrategy(flowStepStrategy);
  }

  @Override
  public List<FlowStep<JobConf>> getFlowSteps() {
    return internalFlow.getFlowSteps();
  }

  @Override
  public void prepare() {
    internalFlow.prepare();
  }

  @Override
  public void start() {
    internalFlow.start();
  }

  @Override
  public void stop() {
    internalFlow.stop();
  }
}
