package com.liveramp.cascading_ext.flow;

import cascading.flow.FlowStep;
import cascading.flow.FlowStepListener;

public class NoOpListener implements FlowStepListener{
  @Override
  public void onStepStarting(FlowStep flowStep) {
    //  no-op
  }

  @Override
  public void onStepStopping(FlowStep flowStep) {
    //  no-op
  }

  @Override
  public void onStepRunning(FlowStep flowStep) {
    //  no-op
  }

  @Override
  public void onStepCompleted(FlowStep flowStep) {
    //  no-op
  }

  @Override
  public boolean onStepThrowable(FlowStep flowStep, Throwable throwable) {
    return false;
  }
}
