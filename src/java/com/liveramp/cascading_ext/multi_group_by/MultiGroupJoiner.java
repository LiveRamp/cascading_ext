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
  protected MultiBuffer buffer;
  protected int groupSize;

  public MultiGroupJoiner(int groupSize, MultiBuffer buffer) {
    this.buffer = buffer;
    this.groupSize = groupSize;
  }

  @Override
  public Iterator<Tuple> getIterator(JoinerClosure joinerClosure) {

    buffer.setContext(groupSize + buffer.getResultFields().size(), (HadoopGroupByClosure) joinerClosure);
    buffer.operate();

    // See comment in CopyingIterator
    return new CopyingIterator(buffer.getResults().iterator());
  }

  public int numJoins() {
    return -1;
  }
}
