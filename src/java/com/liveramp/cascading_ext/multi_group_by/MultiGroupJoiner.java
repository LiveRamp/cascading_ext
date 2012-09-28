package com.liveramp.cascading_ext.multi_group_by;

import cascading.flow.hadoop.HadoopGroupByClosure;
import cascading.pipe.joiner.Joiner;
import cascading.pipe.joiner.JoinerClosure;
import cascading.tuple.Tuple;

import java.util.Iterator;

/**
* @author eddie
*/
public class MultiGroupJoiner implements Joiner {
  protected MultiBuffer _buffer;
  protected int _groupSize;

  public MultiGroupJoiner(int groupSize, MultiBuffer buffer) {
    _buffer = buffer;
    _groupSize = groupSize;
  }

  @Override
  public Iterator<Tuple> getIterator(JoinerClosure joinerClosure) {

    _buffer.setContext(_groupSize + _buffer.getResultFields().size(), (HadoopGroupByClosure) joinerClosure);
    _buffer.operate();

    // See comment in CopyingIterator
    return new CopyingIterator(_buffer.getResults().iterator());
  }

  public int numJoins() {
    return -1;
  }
}
