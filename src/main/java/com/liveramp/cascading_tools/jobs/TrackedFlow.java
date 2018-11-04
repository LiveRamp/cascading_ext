package com.liveramp.cascading_tools.jobs;

import java.util.concurrent.atomic.AtomicBoolean;

import cascading.flow.Flow;

import com.liveramp.cascading_ext.flow.JobPersister;
import com.liveramp.cascading_ext.flow.JobRecordListener;

public class TrackedFlow implements TrackedOperation{

  private final Flow flow;
  private final boolean skipCompleteListener;

  public TrackedFlow(Flow flow, boolean skipCompleteListener){
    this.flow = flow;
    this.skipCompleteListener = skipCompleteListener;
  }

  @Override
  public void complete(JobPersister persister, boolean failOnCounterFetch) {

    AtomicBoolean isComplete = new AtomicBoolean(false);
    flow.addListener(new StopListener(isComplete));
    flow.addStepListener(new JobRecordListener(
        persister,
        failOnCounterFetch
    ));

    flow.complete();

    //  TODO kill skipCompleteListener once we figure out the cascading internal NPE (upgrade past 2.5.1 maybe?)
    if (!isComplete.get() && !skipCompleteListener) {
      throw new RuntimeException("Flow terminated but did not complete!  Possible shutdown hook invocation.");
    }

  }

  @Override
  public void stop() {
    flow.stop();
  }
}
