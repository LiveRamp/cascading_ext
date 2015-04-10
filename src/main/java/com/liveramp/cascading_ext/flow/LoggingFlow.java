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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowException;
import cascading.flow.hadoop.HadoopFlow;
import cascading.flow.planner.PlatformInfo;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopStepStats;

import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.counters.Counters;
import com.liveramp.commons.collections.map.MapBuilder;

/**
 * Delegates actual flow operations to a flow that gets passed in, but performs some additional logging when the job
 * completes or fails.
 */
public class LoggingFlow extends HadoopFlow {
  private static final Pattern LOG_ERROR_PATTERN = Pattern.compile("Caused by.*?\\d\\d\\d\\d-\\d\\d-\\d\\d", Pattern.DOTALL);
  private static Logger LOG = LoggerFactory.getLogger(Flow.class);
  private static final int FAILURES_TO_QUERY = 3;

  private static final Pattern MEMORY_PATTERN = Pattern.compile(".*-Xmx:?([0-9]+)([gGmMkK])?.*");

  private static final Map<String, Long> quantifiers = new MapBuilder<String, Long>()
      .put("g", 1024l * 1024l * 1024l)
      .put("G", 1024l * 1024l * 1024l)
      .put("m", 1024l * 1024l)
      .put("M", 1024l * 1024l)
      .put("k", 1024l)
      .put("K", 1024l)
      .get();

  public LoggingFlow(PlatformInfo platformInfo, java.util.Map<Object, Object> properties, JobConf jobConf, FlowDef flowDef) {
    super(platformInfo, properties, jobConf, flowDef);
    verifyFlowProperties(properties);
  }

  private void verifyFlowProperties(Map<Object, Object> properties) {
    Long maxMem = (Long)properties.get(CascadingUtil.MAX_TASK_MEMORY);

    if (maxMem != null) {
      verifyArgString(properties, JobConf.MAPRED_TASK_JAVA_OPTS, maxMem);
      verifyArgString(properties, JobConf.MAPRED_MAP_TASK_JAVA_OPTS, maxMem);
      verifyArgString(properties, JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS, maxMem);
    }

  }

  private void verifyArgString(Map<Object, Object> properties, String property, long maxMem) {

    String value = (String)properties.get(property);

    if (value != null) {

      Matcher matcher = MEMORY_PATTERN.matcher(value);
      if (!matcher.matches()) {
        throw new RuntimeException("Cannot set property " + property + " without specifying a max heap size!");
      }

      long configured = valueToBytes(matcher);
      if (configured > maxMem) {
        throw new RuntimeException("Job configured memory " + configured + " violates global max " + maxMem + "!");
      }

    }

  }

  private long valueToBytes(Matcher matcher) {
    Long num = Long.parseLong(matcher.group(1));
    if (matcher.groupCount() == 2) {
      return num * quantifiers.get(matcher.group(2));
    } else {
      return num;
    }
  }

  @Override
  public void complete() {
    try {
      super.complete();
      logJobIDs();
      logCounters();
    } catch (FlowException e) {
      logJobIDs();
      String jobErrors = logJobErrors();
      // jobErrors starts with a line delimiter, so prepend it with a newline so that it aligns correctly when printing exceptions
      throw new RuntimeException("\n" + jobErrors, e);
    }
  }

  private void logJobIDs() {
    boolean exceptions = false;
    try {
      List<FlowStepStats> stepStats = getFlowStats().getFlowStepStats();
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
    final String divider = StringUtils.repeat("-", 80);
    logAndAppend(jobErrors, divider);
    try {
      List<FlowStepStats> stepStats = getFlowStats().getFlowStepStats();
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
    logAndAppend(jobErrors, divider);
    return jobErrors.toString();
  }

  private static void logAndAppend(StringBuilder sb, String str) {
    LOG.info(str);
    sb.append(str + "\n");
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

}
