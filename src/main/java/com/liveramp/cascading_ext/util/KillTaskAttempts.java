package com.liveramp.cascading_ext.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

public class KillTaskAttempts {

  public static void main(String[] args) throws IOException {
    run(args);
  }

  //  allow the main jar on the classpath to implement main itself so this can be launched from a jobjar
  public static void run(String[] args) throws IOException {

    if(args.length != 3 || !(args[1].equals("map") || args[1].equals("reduce"))){
      System.out.println("Usage: <job pattern to target> <map|reduce> <tasks to kill>");
      System.exit(0);
    }

    String jobsToTarget = args[0];
    Boolean killMap = args[1].equals("map");
    Integer toKill = Integer.parseInt(args[2]);

    System.out.println();
    System.out.println("Target jobs named: "+jobsToTarget);
    System.out.println("Kill tasks of type: "+args[1]);
    System.out.println("Number of tasks to kill: "+toKill);
    System.out.println("---------------------------- WARNING ---------------------------");
    System.out.println("Killing running tasks comes at a serious performance penalty for targeted jobs.");
    System.out.println("Only continue if you REALLY know what you are doing");
    System.out.println("Press Enter to confirm:");

    new Scanner(System.in).nextLine();

    JobClient jobClient = new JobClient(new InetSocketAddress("ds-jt.rapleaf.com", 7277), new Configuration());

    Map<TaskAttemptID, Float> taskAttemptToProgress = Maps.newHashMap();

    for(JobStatus status: jobClient.getAllJobs()){
      if(status.getRunState() == JobStatus.RUNNING){
        if(jobClient.getJob(status.getJobID()).getJobName().contains(jobsToTarget)){
          JobID jobid = status.getJobID();

          TaskReport[] reports;
          if(killMap){
            reports = jobClient.getMapTaskReports(jobid);
          }else{
            reports = jobClient.getReduceTaskReports(jobid);
          }

          for(TaskReport report: reports){
            Collection<TaskAttemptID> running = report.getRunningTaskAttempts();
            for(TaskAttemptID attempt: running){
              taskAttemptToProgress.put(attempt, report.getProgress());
            }
          }
        }
      }
    }

    List<Map.Entry<TaskAttemptID, Float>> attemptToProgress = Lists.newArrayList(taskAttemptToProgress.entrySet());
    Collections.sort(attemptToProgress, new Comparator<Map.Entry<TaskAttemptID, Float>>() {
      @Override
      public int compare(Map.Entry<TaskAttemptID, Float> o1, Map.Entry<TaskAttemptID, Float> o2) {
        return (int) (1000f * (o1.getValue() - o2.getValue()));
      }
    });

    int killed = 0;
    for(Map.Entry<TaskAttemptID, Float> entry: attemptToProgress){
      if(killed >= toKill){
        break;
      }

      System.out.println("Killing task attempt: "+entry.getKey()+", progress = "+entry.getValue());
      RunningJob runningJob = jobClient.getJob(entry.getKey().getJobID());
      runningJob.killTask(entry.getKey(), false);

      killed++;
    }
    System.out.println("Killed "+killed+" total task attempts");
  }
}
