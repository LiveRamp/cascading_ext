package com.liveramp.cascading_ext.multi_group_by;

import cascading.tuple.Tuple;

import java.util.Iterator;

/**
 * Decorator that creates copies of tuples returned by the decorated class. This is
 * a workaround for (what's possibly) a bug in cascading2. bpod is talking with cwensel
 * about a less hacky fix.
 */
public class CopyingIterator implements Iterator<Tuple> {
  private final Iterator<Tuple> innerIterator;

  public CopyingIterator(Iterator<Tuple> innerIterator) {
    this.innerIterator = innerIterator;
  }

  @Override
  public boolean hasNext() {
    return innerIterator.hasNext();
  }

  @Override
  public Tuple next() {
    return new Tuple(innerIterator.next());
  }

  @Override
  public void remove() {
    innerIterator.remove();
  }
}
