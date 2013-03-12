package com.liveramp.cascading_ext.assembly;


import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import com.liveramp.cascading_ext.operation.IncrementCounterForFieldValues;

public class IncrementForFieldValues extends SubAssembly{

  public IncrementForFieldValues(Pipe pipe, String counterName, Fields fields, Object... values) {
    setTails(new Each(pipe, new IncrementCounterForFieldValues(counterName,fields, values)));
  }

  public IncrementForFieldValues(Pipe pipe, String counterGroup, String counterName, Fields fields, Object... values) {
    setTails(new Each(pipe, new IncrementCounterForFieldValues(counterGroup, counterName, fields, values)));
  }

  public IncrementForFieldValues(Pipe pipe, Enum counter, Fields fields, Object... values) {
    setTails(new Each(pipe, new IncrementCounterForFieldValues(counter, fields, values)));
  }

}
