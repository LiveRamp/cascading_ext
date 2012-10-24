package com.liveramp.cascading_ext.assembly;

import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 * This SubAssembly filters the LHS against a bloom filter created from the keys on the RHS.
 */
public class BloomFilter extends BloomAssembly {
  /**
   * Retain only the tuples on the LHS ("largePipe") with selected fields that appear also on the RHS ("smallPipe").
   * This is accomplished by running the LHS against a bloom filter built from the fields on the RHS. As such, the side
   * with relatively fewer tuples should always be passed in as the RHS.
   * <p/>
   * It's possible to do an exact or inexact filter. <b>An inexact filter will have a small number of false
   * positives</b> (e.g., tuples on the LHS that aren't on the RHS), but doesn't require a CoGroup at the end. An exact
   * filter is generally faster than doing a CoGroup by itself because it filters the LHS with a bloom filter.
   *
   * @param largePipe       the left hand side of the bloom operation. this should be the side that the bloom filter will be
   *                        run against. In other words, this should be the side with relatively more tuples.
   * @param largeJoinFields the fields on the left hand side that will be compared with the right hand side
   * @param smallPipe       the right hand side of the bloom operation. this should be the side that we build the bloom filter
   *                        against. In other words, this should be the side with relatively fewer tuples.
   * @param smallJoinFields the fields on the right hand side that will be compared with the left hand side
   * @param exact           if true, perform a CoGroup after applying the bloom filter to the LHS. This will eliminate false
   *                        positives.
   */
  public BloomFilter(Pipe largePipe, Fields largeJoinFields, Pipe smallPipe, Fields smallJoinFields, boolean exact) {
    this(largePipe, largeJoinFields, smallPipe, smallJoinFields, exact, CoGroupOrder.LARGE_LHS);
  }

  public BloomFilter(Pipe largePipe, Fields largeJoinFields, Pipe smallPipe, Fields smallJoinFields, boolean exact, CoGroupOrder coGroupOrder) {
    super(largePipe, largeJoinFields, smallPipe, smallJoinFields, null, exact ? Mode.FILTER_EXACT : Mode.FILTER_INEXACT, null, coGroupOrder);
  }
}
