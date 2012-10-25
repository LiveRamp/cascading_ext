package com.liveramp.cascading_ext.assembly;

import cascading.pipe.Pipe;
import cascading.pipe.joiner.Joiner;
import cascading.tuple.Fields;

/**
 * This SubAssembly behaves almost exactly like CoGroup, except that the LHS is filtered using a bloom filter
 * built from the keys on the RHS.  This means that the side with fewer keys should always be passed in as the RHS.
 *
 * Note that if there are field name conflicts on the LHS and RHS, you'll have to pass in renameFields (just like
 * with CoGroup).  Additionally, @param coGroupOrder allows tweaking of reduce performance (default is LARGE_LHS).
 *
 * IMPORTANT: one important behavior difference between BloomJoin and CoGroup is that RHS and LHS keys which are expected
 * to match in the join MUST serialize identically as well (the bloom filter is built by serializing key fields.)  If
 * normal java/hadoop types are used this should not be a problem, but comparing key types which extend each other WILL
 * cause data loss, unless customer serializers are used.  ex, LHS = (BytesWritable), RHS = (x extends BytesWritable)
 * loses data when the types serialize differently.
 */
public class BloomJoin extends BloomAssembly {

  /**
   *
   * @param largePipe       the left hand side of the bloom operation. this will be the side that the bloom filter will be
   *                        run against. Usually, this should be the side with relatively more tuples.
   * @param largeJoinFields the fields on the left hand side that will be compared with the right hand side
   * @param smallPipe       the right hand side of the bloom operation. this willbe the side that we build the bloom filter
   *                        against. Usually, this should be the side with relatively fewer tuples.
   * @param smallJoinFields the fields on the right hand side that will be compared with the left hand side
   * @param renameFields    if doing a join and the LHS and RHS have some of the same field names, you'll need to supply
   *                        rename fields (just like with CoGroup).
   * @param joiner          allow a specific joiner to be used in the final exact join
   * @param coGroupOrder    in some cases, one pipe has a smaller cardinality but a larger number of tuples overall (some values
   *                        have high duplication.)  In this case, these values should be in the bloom filter, but also should be
   *                        the LHS in the final CoGroup since they spill more frequently.  In this case, CoGroupOrder.LARGE_RHS
   *                        should be used
   */
  public BloomJoin(Pipe largePipe, Fields largeJoinFields, Pipe smallPipe, Fields smallJoinFields, Fields renameFields, Joiner joiner, CoGroupOrder coGroupOrder) {
    super(largePipe, largeJoinFields, smallPipe, smallJoinFields, renameFields, Mode.JOIN, joiner, coGroupOrder);
  }

  public BloomJoin(Pipe largePipe, Fields largeJoinFields, Pipe smallPipe, Fields smallJoinFields, Fields renameFields) {
    this(largePipe, largeJoinFields, smallPipe, smallJoinFields, renameFields, CoGroupOrder.LARGE_LHS);
  }

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

  public BloomJoin(Pipe largePipe, Fields largeJoinFields, Pipe smallPipe, Fields smallJoinFields, Joiner joiner, CoGroupOrder coGroupOrder) {
    super(largePipe, largeJoinFields, smallPipe, smallJoinFields, null, Mode.JOIN, joiner, coGroupOrder);
  }
}
