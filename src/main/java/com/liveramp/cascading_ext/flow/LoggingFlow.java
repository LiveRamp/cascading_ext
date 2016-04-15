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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskCompletionEvent.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlow;
import cascading.flow.planner.PlatformInfo;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopStepStats;

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
  }

  @Override
  public void complete() {
    try {
      super.complete();
      logJobIDs();
      logCounters();
    } catch (Exception e) {
      logJobIDs();
      long start = System.currentTimeMillis();
      String jobErrors = logJobErrors();
      LOG.info("Failure log collection took "+(System.currentTimeMillis()-start)+" millis: \n");
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
          // We limit the number of potential logs being pulled since we don't want to spend forever on these queries
          if (failures.size() > 0) {
            Collections.shuffle(failures);
            for (int i = 0; i < FAILURES_TO_QUERY; i++) {
              Collections.addAll(jobFailures, job.getTaskDiagnostics((failures.get(i).getTaskAttemptId())));
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
    sb.append(str).append("\n");
  }

  private void logCounters() throws IOException {
    LOG.info(Counters.prettyCountersString(this));
  }

}
