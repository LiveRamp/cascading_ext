/**
 *  Copyright 2012 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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
