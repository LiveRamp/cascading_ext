package com.liveramp.cascading_tools.jobs;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.Flow;

import com.liveramp.cascading_tools.EmptyListener;

public class StopListener extends EmptyListener {
  private static final Logger LOG = LoggerFactory.getLogger(StopListener.class);

  private final AtomicBoolean isComplete;

  public StopListener(AtomicBoolean isComplete) {
    this.isComplete = isComplete;
  }

  @Override
  public void onCompleted(Flow flow) {
    LOG.info("Flow onCompleted called");
    isComplete.set(true);
  }

  @Override
  public void onStarting(Flow flow) {
    LOG.info("Flow onStarting called");
  }

  @Override
  public void onStopping(Flow flow) {
    LOG.info("Flow onStopping  called");
  }

  @Override
  public boolean onThrowable(Flow flow, Throwable throwable) {
    LOG.info("Flow onThrowable called: ", throwable);
    return false;
  }

}
