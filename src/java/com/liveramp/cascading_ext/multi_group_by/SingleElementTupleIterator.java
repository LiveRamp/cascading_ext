package com.liveramp.cascading_ext.multi_group_by;

import cascading.tuple.Tuple;

import java.util.Iterator;

/**
 * An iterator (that wraps an Iterator<Tuple>) that iterates over a single position in a stream of tuples.
 *
 * @param <T>
 */
public class SingleElementTupleIterator<T> implements Iterator<T> {

  private final Iterator<Tuple> iter;
  private final int value;

  public SingleElementTupleIterator(Iterator<Tuple> interior, int pos) {
    this.iter = interior;
    this.value = pos;
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public T next() {
    Tuple t = iter.next();
    return (T) t.getObject(value);
  }

  @Override
  public void remove() {
    iter.remove();
  }
}
