package com.liveramp.cascading_tools.jobs;

import java.io.IOException;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskReport;

import com.liveramp.java_support.workflow.TaskSummary;

public class JobUtil {

  public static TaskSummary getSummary(JobClient client, JobID id) throws IOException {
    DescriptiveStatistics mapStats = getRuntimes(client.getMapTaskReports(id));
    DescriptiveStatistics reduceStats = getRuntimes(client.getReduceTaskReports(id));
    return new TaskSummary((long)mapStats.getMean(),
        (long)mapStats.getPercentile(50),
        (long)mapStats.getMax(),
        (long)mapStats.getMin(),
        (long)mapStats.getStandardDeviation(),
        (long)reduceStats.getMean(),
        (long)reduceStats.getPercentile(50),
        (long)reduceStats.getMax(),
        (long)reduceStats.getMin(),
        (long)reduceStats.getStandardDeviation()
    );
  }

  private static DescriptiveStatistics getRuntimes(TaskReport[] reports) {
    DescriptiveStatistics stats = new DescriptiveStatistics();
    for (TaskReport report : reports) {
      if (report != null) {
        if (report.getCurrentStatus() == TIPStatus.COMPLETE) {
          stats.addValue(report.getFinishTime() - report.getStartTime());
        }
      }
    }
    return stats;
  }


}
