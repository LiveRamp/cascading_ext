package com.liveramp.cascading_ext.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;


public class IncrementCounterForFieldValues extends BaseOperation implements Filter {

  private final String counterGroup;
  private final String counterName;
  private final Enum counterEnum;
  private final Fields fields;
  private final Object[] values;

  public IncrementCounterForFieldValues(String counterName, Fields fields, Object... values) {
    this.fields = fields;
    this.values = values;
    this.counterGroup = "";
    this.counterName = counterName;
    this.counterEnum = null;
  }

  public IncrementCounterForFieldValues(String counterGroup, String counterName, Fields fields, Object... values) {
    this.counterGroup = counterGroup;
    this.counterName = counterName;
    this.fields = fields;
    this.values = values;
    this.counterEnum = null;
  }

  public IncrementCounterForFieldValues(Enum counter, Fields fields, Object... values) {
    this.fields = fields;
    this.values = values;
    this.counterGroup = null;
    this.counterName = null;
    this.counterEnum = counter;
  }

  @Override
  public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
    boolean fieldsEqualValues = true;

    TupleEntry arguments = filterCall.getArguments();
    for (int index = 0; index < fields.size(); index++) {
      Object object = arguments.getObject(fields.get(index));
      Object value = values[index];
      if (!equalOrBothNull(object, value)) {
        fieldsEqualValues = false;
        break;
      }
    }

    if (fieldsEqualValues) {
      if (counterEnum == null) {
        flowProcess.increment(counterGroup, counterName, 1);
      } else {
        flowProcess.increment(counterEnum, 1);
      }
    }
    return false;
  }

  private boolean equalOrBothNull(Object object, Object value) {
    return ((object == null && value == null) ||
        (object != null && object.equals(value)));
  }
}
