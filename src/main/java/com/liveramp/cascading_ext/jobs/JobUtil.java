package com.liveramp.cascading_ext.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskType;

import com.liveramp.commons.state.TaskFailure;
import com.liveramp.commons.state.TaskSummary;

public class JobUtil {

  private static int FAILURES_TO_QUERY = 3;
  private static int MILLIS_TO_SEARCH = 5000; //this will limit the length of searches when searchUntilFound is false

  public static TaskSummary getSummary(Job job, boolean searchUntilFound) throws IOException, InterruptedException {
    DescriptiveStatistics mapStats = getRuntimes(job.getTaskReports(TaskType.MAP));
    DescriptiveStatistics reduceStats = getRuntimes(job.getTaskReports(TaskType.REDUCE));

    return getSummary(mapStats, reduceStats, getTaskFailures(new JobFetch(job), searchUntilFound));
  }

  public static TaskSummary getSummary(JobClient client, JobID id, boolean searchUntilFound) throws IOException, InterruptedException {
    DescriptiveStatistics mapStats = getRuntimes(client.getMapTaskReports(id));
    DescriptiveStatistics reduceStats = getRuntimes(client.getReduceTaskReports(id));

    return getSummary(mapStats, reduceStats, getTaskFailures(new RunningJobFetch(client.getJob(id)), searchUntilFound));
  }


  private static TaskSummary getSummary(DescriptiveStatistics mapStats, DescriptiveStatistics reduceStats, List<TaskFailure> taskFailures) {
    return new TaskSummary((long)mapStats.getMean(),
        (long)mapStats.getPercentile(50),
        (long)mapStats.getMax(),
        (long)mapStats.getMin(),
        (long)mapStats.getStandardDeviation(),
        (long)reduceStats.getMean(),
        (long)reduceStats.getPercentile(50),
        (long)reduceStats.getMax(),
        (long)reduceStats.getMin(),
        (long)reduceStats.getStandardDeviation(),
        taskFailures
    );
  }

  //  this is to paper over the new vs old APIs.  feel like there should be a better way, but I don't see it.
  interface Fetch{
    public org.apache.hadoop.mapred.TaskCompletionEvent[] getTaskCompletionEvents(int index) throws IOException;
    public String[] getTaskDiagnostics(org.apache.hadoop.mapred.TaskAttemptID taskAttemptID) throws IOException, InterruptedException;
  }

  public static class RunningJobFetch implements Fetch{

    RunningJob job;
    public RunningJobFetch(RunningJob job){
      this.job = job;
    }

    @Override
    public TaskCompletionEvent[] getTaskCompletionEvents(int index) throws IOException {
      return job.getTaskCompletionEvents(index);
    }

    @Override
    public String[] getTaskDiagnostics(org.apache.hadoop.mapred.TaskAttemptID taskAttemptID) throws IOException {
      return job.getTaskDiagnostics(taskAttemptID);
    }
  }

  public static class JobFetch implements Fetch{

    Job job;
    public JobFetch(Job job){
      this.job = job;
    }

    @Override
    public TaskCompletionEvent[] getTaskCompletionEvents(int index) throws IOException {
      return job.getTaskCompletionEvents(index);
    }

    @Override
    public String[] getTaskDiagnostics(org.apache.hadoop.mapred.TaskAttemptID taskAttemptID) throws IOException, InterruptedException {
      return job.getTaskDiagnostics(taskAttemptID);
    }
  }

  private static List<TaskFailure> getTaskFailures(Fetch job, boolean searchUntilFound) throws IOException, InterruptedException {
    List<TaskFailure> jobFailures = new ArrayList<>();

    long start = System.currentTimeMillis();

    int index = 0;
    TaskCompletionEvent[] events = job.getTaskCompletionEvents(index);
    ArrayList<TaskCompletionEvent> failures = new ArrayList<>();

    //this returns either nothing (if no task exceptions at all) or a subset of the exceptions from the first
    //index at which exceptions are found in the task completion events
    //we also limit the time spent on it, in case a completely successful job with too many tasks
    while (
        events.length > 0
            && failures.size() == 0
            && (searchUntilFound || System.currentTimeMillis() - start < MILLIS_TO_SEARCH)) {
      for (TaskCompletionEvent event : events) {
        if (event.getTaskStatus() == TaskCompletionEvent.Status.FAILED) {
          failures.add(event);
        }
      }
      index += 10; //hadoop is weird and caps the number of task events at 10 non-optionally
      events = job.getTaskCompletionEvents(index);
    }

    // We limit the number of potential logs being pulled since we don't want to spend forever on these queries
    if (failures.size() > 0) {
      Collections.shuffle(failures);
      int num_queries = Math.min(FAILURES_TO_QUERY, failures.size());
      for (int i = 0; i < num_queries; i++) {
        TaskAttemptID taskAttemptID = failures.get(i).getTaskAttemptId();
        String[] fails = job.getTaskDiagnostics(taskAttemptID);
        for (String failure : fails) {
          jobFailures.add(new TaskFailure(taskAttemptID.toString(), failure));
        }
      }
    }
    return jobFailures;
  }


  private static DescriptiveStatistics getRuntimes(org.apache.hadoop.mapreduce.TaskReport[] reports) {
    DescriptiveStatistics stats = new DescriptiveStatistics();
    for (org.apache.hadoop.mapreduce.TaskReport report : reports) {
      if (report != null) {
        if (report.getCurrentStatus() == TIPStatus.COMPLETE) {
          stats.addValue(report.getFinishTime() - report.getStartTime());
        }
      }
    }
    return stats;
  }


}
