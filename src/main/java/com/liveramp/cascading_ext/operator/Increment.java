package com.liveramp.cascading_ext.operator;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import com.liveramp.cascading_ext.operation.IncrementCounter;

public class Increment extends Each {

  public Increment(Pipe pipe, String counterName) {
    super(pipe, new IncrementCounter(counterName));
  }

  public Increment(Pipe pipe, String counterGroup, String counterName) {
    super(pipe, new IncrementCounter(counterGroup, counterName));
  }

  public Increment(Pipe pipe, Enum counter) {
    super(pipe, new IncrementCounter(counter));
  }

}
