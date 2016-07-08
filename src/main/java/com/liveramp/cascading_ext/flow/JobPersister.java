package com.liveramp.cascading_ext.flow;

import java.io.IOException;

import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.commons.state.LaunchedJob;
import com.liveramp.commons.state.TaskSummary;

public interface JobPersister {
  void onRunning(LaunchedJob launchedJob) throws IOException;

  void onTaskInfo(String jobID, TaskSummary summary) throws IOException;

  void onCounters(String jobID, TwoNestedMap<String, String, Long> counters) throws IOException;

  public static class NoOp implements JobPersister {

    @Override
    public void onRunning(LaunchedJob launchedJob) throws IOException {
      //  no-op
    }

    @Override
    public void onTaskInfo(String jobID, TaskSummary summary) throws IOException {
      //  no-op
    }

    @Override
    public void onCounters(String jobID, TwoNestedMap<String, String, Long> counters) throws IOException {
      //  no-op
    }
  }

}
