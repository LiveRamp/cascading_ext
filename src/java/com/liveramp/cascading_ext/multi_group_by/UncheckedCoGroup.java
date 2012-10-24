package com.liveramp.cascading_ext.multi_group_by;

import cascading.flow.planner.Scope;
import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.Joiner;
import cascading.tuple.Fields;

/**
 * @author eddie
 */
public class UncheckedCoGroup extends CoGroup {

  public UncheckedCoGroup(Pipe[] pipes,
                          Fields[] groupFields,
                          Fields declaredFields,
                          Joiner joiner) {
    super(pipes, groupFields, declaredFields, joiner);
  }

  public Fields resolveFields(Scope scope) {
    return NaiveFields.fromFields(super.resolveFields(scope));
  }
}
