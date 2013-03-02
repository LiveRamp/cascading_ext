package com.liveramp.cascading_ext.operator;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import com.liveramp.cascading_ext.operation.IncrementCounter;

public class Increment extends SubAssembly {

  public Increment(Pipe pipe, String counterName) {
    setTails(new Each(pipe, new IncrementCounter(counterName)));
  }

  public Increment(Pipe pipe, String counterGroup, String counterName) {
    setTails(new Each(pipe, new IncrementCounter(counterGroup, counterName)));
  }

  public Increment(Pipe pipe, Enum counter) {
    setTails(new Each(pipe, new IncrementCounter(counter)));
  }
}
