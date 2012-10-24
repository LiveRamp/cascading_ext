package com.liveramp.cascading_ext.joiner;

import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.JoinerClosure;
import cascading.tuple.Tuple;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class LimitJoin extends InnerJoin {

  private final long[] streamsToLimit;

  public LimitJoin(long[] streamsToLimit) {
    this.streamsToLimit = streamsToLimit;
  }

  public Iterator<Tuple> getIterator(JoinerClosure closure) {
    return new JoinLimitIterator(closure, streamsToLimit);
  }

  public static class JoinLimitIterator extends InnerJoin.JoinIterator {

    //  private, so hold onto it here
    private final JoinerClosure closureLocal;
    private final long[] limitedStreams;

    public JoinLimitIterator(JoinerClosure closure, long[] streamsToLimit) {
      super(closure);
      this.closureLocal = closure;
      this.limitedStreams = streamsToLimit;
    }

    protected Iterator getIterator(int i) {
      return new LimitedIterator(i);
    }

    private class LimitedIterator implements Iterator<Tuple> {

      private Iterator<Tuple> internal;
      private long numReturned = 0;
      private final int iteratorNum;

      public LimitedIterator(int iteratorNum) {
        this.iteratorNum = iteratorNum;
      }

      private void checkIter() {
        if (internal == null) {
          internal = closureLocal.getIterator(iteratorNum);
        }
      }

      private boolean isDone() {
        return numReturned == limitedStreams[iteratorNum];
      }

      @Override
      public boolean hasNext() {
        checkIter();
        return !isDone() && internal.hasNext();
      }

      @Override
      public Tuple next() {
        checkIter();
        if (isDone()) {
          throw new NoSuchElementException();
        }
        numReturned++;
        return internal.next();
      }

      @Override
      public void remove() {
        checkIter();
        internal.remove();
      }
    }
  }
}
