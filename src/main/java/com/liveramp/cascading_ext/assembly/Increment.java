package com.liveramp.cascading_ext.assembly;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import com.liveramp.cascading_ext.operation.IncrementCounter;

public class Increment extends SubAssembly {

  public Increment(Pipe pipe, String counterName) {
    super(pipe);
    setTails(new Each(pipe, new IncrementCounter(counterName)));
  }

  public Increment(Pipe pipe, String counterGroup, String counterName) {
    super(pipe);
    setTails(new Each(pipe, new IncrementCounter(counterGroup, counterName)));
  }

  public Increment(Pipe pipe, Enum counter) {
    super(pipe);
    setTails(new Each(pipe, new IncrementCounter(counter)));
  }
}
