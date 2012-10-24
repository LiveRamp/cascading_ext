package com.liveramp.cascading_ext.assembly;

import cascading.pipe.Pipe;
import cascading.pipe.joiner.Joiner;
import cascading.tuple.Fields;

/**
 * This SubAssembly behaves exactly like CoGroup, except that the LHS is filtered using a bloom filter
 * built from the keys on the RHS.
 */
public class BloomJoin extends BloomAssembly {

  /**
   * This behaves exactly like CoGroup, except that the LHS ("largePipe") is filtered using a bloom filter built from
   * the RHS ("smallPipe"). This means that the side with fewer keys should always be passed in as the RHS.
   *
   * @param largePipe       the left hand side of the bloom operation. this should be the side that the bloom filter will be
   *                        run against. In other words, this should be the side with relatively more tuples.
   * @param largeJoinFields the fields on the left hand side that will be compared with the right hand side
   * @param smallPipe       the right hand side of the bloom operation. this should be the side that we build the bloom filter
   *                        against. In other words, this should be the side with relatively fewer tuples.
   * @param smallJoinFields the fields on the right hand side that will be compared with the left hand side
   * @param renameFields    if doing a join and the LHS and RHS have some of the same field names, you'll need to supply
   *                        rename fields (just like with CoGroup).
   */
  public BloomJoin(Pipe largePipe, Fields largeJoinFields, Pipe smallPipe, Fields smallJoinFields, Fields renameFields) {
    this(largePipe, largeJoinFields, smallPipe, smallJoinFields, renameFields, CoGroupOrder.LARGE_LHS);
  }

  /**
   * This behaves exactly like CoGroup, except that the LHS ("largePipe") is filtered using a bloom filter built from
   * the RHS ("smallPipe"). This means that the side with fewer keys should always be passed in as the RHS.
   * <p/>
   * Note that if there are field name conflicts on the LHS and RHS, you'll have to pass in renameFields (just like
   * with CoGroup).
   *
   * @param largePipe       the left hand side of the bloom operation. this should be the side that the bloom filter will be
   *                        run against. In other words, this should be the side with relatively more tuples.
   * @param largeJoinFields the fields on the left hand side that will be compared with the right hand side
   * @param smallPipe       the right hand side of the bloom operation. this should be the side that we build the bloom filter
   *                        against. In other words, this should be the side with relatively fewer tuples.
   * @param smallJoinFields the fields on the right hand side that will be compared with the left hand side
   */
  public BloomJoin(Pipe largePipe, Fields largeJoinFields, Pipe smallPipe, Fields smallJoinFields) {
    this(largePipe, largeJoinFields, smallPipe, smallJoinFields, null, null, CoGroupOrder.LARGE_LHS);
  }

  public BloomJoin(Pipe largePipe, Fields largeJoinFields, Pipe smallPipe, Fields smallJoinFields, Fields renameFields, Joiner joiner) {
    this(largePipe, largeJoinFields, smallPipe, smallJoinFields, renameFields, joiner, CoGroupOrder.LARGE_LHS);
  }

  public BloomJoin(Pipe largePipe, Fields largeJoinFields, Pipe smallPipe, Fields smallJoinFields, Joiner joiner) {
    this(largePipe, largeJoinFields, smallPipe, smallJoinFields, null, joiner, CoGroupOrder.LARGE_LHS);
  }

  public BloomJoin(Pipe largePipe, Fields largeJoinFields, Pipe smallPipe, Fields smallJoinFields, Fields renameFields, CoGroupOrder coGroupOrder) {
    this(largePipe, largeJoinFields, smallPipe, smallJoinFields, renameFields, null, coGroupOrder);
  }

  public BloomJoin(Pipe largePipe, Fields largeJoinFields, Pipe smallPipe, Fields smallJoinFields, CoGroupOrder coGroupOrder) {
    this(largePipe, largeJoinFields, smallPipe, smallJoinFields, null, null, coGroupOrder);
  }

  public BloomJoin(Pipe largePipe, Fields largeJoinFields, Pipe smallPipe, Fields smallJoinFields, Fields renameFields, Joiner joiner, CoGroupOrder coGroupOrder) {
    super(largePipe, largeJoinFields, smallPipe, smallJoinFields, renameFields, Mode.JOIN, joiner, coGroupOrder);
  }

  public BloomJoin(Pipe largePipe, Fields largeJoinFields, Pipe smallPipe, Fields smallJoinFields, Joiner joiner, CoGroupOrder coGroupOrder) {
    super(largePipe, largeJoinFields, smallPipe, smallJoinFields, null, Mode.JOIN, joiner, coGroupOrder);
  }

}
