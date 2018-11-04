package com.liveramp.cascading_tools;

import cascading.flow.Flow;
import cascading.flow.FlowListener;

// no op
public class EmptyListener implements FlowListener {

  @Override
  public void onStarting(Flow flow) {
    // no op
  }

  @Override
  public void onStopping(Flow flow) {
    // no op
  }

  @Override
  public void onCompleted(Flow flow) {
    // no op
  }

  @Override
  public boolean onThrowable(Flow flow, Throwable throwable) {
    return false;
  }
}
