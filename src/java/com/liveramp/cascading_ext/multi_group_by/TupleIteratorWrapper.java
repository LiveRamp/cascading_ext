package com.liveramp.cascading_ext.multi_group_by;

import cascading.tuple.Tuple;

import java.util.Iterator;

public class TupleIteratorWrapper<T> implements Iterator<T> {

  private final Iterator<Tuple> iter;
  private final int value;

  public TupleIteratorWrapper(Iterator<Tuple> interior) {
    this(interior, 0);
  }

  public TupleIteratorWrapper(Iterator<Tuple> interior, int field) {
    this.iter = interior;
    this.value = field;
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public T next() {
    Tuple t = iter.next();
    return (T) t.get(value);
  }

  @Override
  public void remove() {
    iter.remove();
  }
}
