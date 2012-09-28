package com.liveramp.cascading_ext.multi_group_by;

import cascading.tuple.Fields;

public class NaiveFields extends Fields {
  public NaiveFields(Comparable... fields) {
    super(fields);
  }

  public static Fields fromFields(Fields fields) {
    Comparable[] comps = new Comparable[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      comps[i] = fields.get(i);
    }
    return new NaiveFields(comps);
  }

  @Override
  public boolean isUnknown() {
    return true;
  }
}
