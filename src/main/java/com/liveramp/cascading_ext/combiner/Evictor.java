package com.liveramp.cascading_ext.combiner;

import java.io.Serializable;

public interface Evictor<T> extends Serializable {

  public boolean shouldEvict(T aggregate);
}
