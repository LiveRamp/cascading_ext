package com.liveramp.cascading_tools.jobs;

import com.liveramp.cascading_ext.flow.JobPersister;

public interface TrackedOperation {
  public void complete(JobPersister persister, boolean failOnCounterFetch);

  default void completeWithNoTracking() {
    complete(new JobPersister.NoOp(), false);
  }

  public void stop();

  class NoOp implements TrackedOperation {
    @Override
    public void complete(JobPersister persister, boolean failOnCounterFetch) {

    }

    @Override
    public void stop() {

    }
  }
}
