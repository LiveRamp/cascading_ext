package com.liveramp.cascading_ext;

import cascading.flow.*;
import cascading.management.UnitOfWorkSpawnStrategy;
import cascading.stats.FlowStats;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopStepStats;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class LoggingFlow implements Flow<JobConf> {
  private static Logger LOG = Logger.getLogger(Flow.class);

  private final Flow<JobConf> internalFlow;

  public LoggingFlow(Flow<JobConf> internalFlow, FlowStepStrategy flowStepStrategy) {
    this.internalFlow = internalFlow;
    this.internalFlow.setFlowStepStrategy(flowStepStrategy);
  }

  @Override
  public void complete() {
    try {
      internalFlow.complete();
      logJobIDs();
      logCounters();
    } catch (FlowException e) {
      logJobIDs();
      throw e;
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
          JobID jobid = ((HadoopStepStats) stat).getRunningJob().getID();
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
  public FlowProcess getFlowProcess() {
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
