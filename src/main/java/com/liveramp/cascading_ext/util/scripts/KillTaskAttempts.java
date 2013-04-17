package com.liveramp.cascading_ext.util.scripts;

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

    if(args.length != 5 || !(args[3].equals("map") || args[3].equals("reduce"))){
      System.out.println("Usage: <job pattern to target> <map|reduce> <tasks to kill>");
      System.exit(0);
    }

    String jobTracker = args[0];
    Integer port = Integer.parseInt(args[1]);
    String jobsToTarget = args[2];
    Boolean killMap = args[3].equals("map");
    Integer toKill = Integer.parseInt(args[4]);

    System.out.println();
    System.out.println("Job tracker host: "+jobTracker);
    System.out.println("Job tracker port: "+port);
    System.out.println("Target jobs named: "+jobsToTarget);
    System.out.println("Kill tasks of type: "+args[3]);
    System.out.println("Number of tasks to kill: "+toKill);
    System.out.println("---------------------------- WARNING ---------------------------");
    System.out.println("Killing running tasks comes at a serious performance penalty for targeted jobs.");
    System.out.println("Only continue if you REALLY know what you are doing");
    System.out.println("Press Enter to confirm:");

    new Scanner(System.in).nextLine();

    JobClient jobClient = new JobClient(new InetSocketAddress(jobTracker, port), new Configuration());

    Map<TaskAttemptID, Float> taskAttemptToProgress = Maps.newHashMap();

    for(JobStatus status: jobClient.getAllJobs()){
      if(status.getRunState() == JobStatus.RUNNING){
        RunningJob job = jobClient.getJob(status.getJobID());
        if(job.getJobName().contains(jobsToTarget) || job.getID().toString().contains(jobsToTarget)){
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
