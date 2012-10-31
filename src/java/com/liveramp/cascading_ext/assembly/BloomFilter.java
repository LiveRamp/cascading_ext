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

package com.liveramp.cascading_ext.assembly;

import cascading.pipe.Pipe;
import cascading.tuple.Fields;

/**
 * This SubAssembly filters the LHS ("largePipe") to retain only the tuples with selected fields that appear also in
 * the RHS ("smallPipe").  This is accomplished by running the LHS against a bloom filter built from the fields on the
 * RHS. As such, the side with lower cardinality should be passed in as the RHS.
 *
 * It's possible to do an exact or inexact filter. <b>An inexact filter will have a small number of false
 * positives</b> (e.g., tuples on the LHS that aren't on the RHS), but doesn't require a CoGroup at the end. An exact
 * filter is generally faster than doing a CoGroup by itself because it filters the LHS with a bloom filter.
 *
 * Additionally, @param coGroupOrder allows tweaking of reduce performance, see constructor (default is LARGE_LHS).
 *
 * Most of the functionality of BloomFilter (except for the ability to do inexact joins) is available via
 * com.liveramp.cascading_ext.assembly.BloomJoin as well.  However, by discarding all RHS fields after the join, the
 * Fields algebra is easier to manage (no need to deal with conflicting field names.)
 *
 * <b>IMPORTANT:</b> the same warning about serialization in BloomJoin also applies here.  See
 * com.liveramp.cascading_ext.assembly.BloomJoin for details.
 */
public class BloomFilter extends BloomAssembly {

  /**
   * @param largePipe       the left hand side of the bloom operation. this will be the side that is filtered by
   *                        the bloom filter.  Usually, this should be the side with more tuples.
   * @param largeJoinFields the fields on the left hand side that will be compared with the right hand side
   * @param smallPipe       the right hand side of the bloom operation. this will be the side that is used to build the
   *                        bloom filter. Usually, this should be the side with fewer tuples.
   * @param smallJoinFields the fields on the right hand side that will be compared with the left hand side
   * @param exact           if true, perform a CoGroup after applying the bloom filter to the LHS. This will eliminate false
   *                        positives.
   * @param coGroupOrder    in some cases, one pipe has a smaller cardinality but a larger number of tuples overall (some values
   *                        have high duplication.)  In this case, these values should be in the bloom filter, so should be in
   *                        "smallPipe", but also should be the LHS in the final CoGroup since they spill more frequently.
   *                        In this case, CoGroupOrder.LARGE_RHS should be used
   */
  public BloomFilter(Pipe largePipe, Fields largeJoinFields, Pipe smallPipe, Fields smallJoinFields, boolean exact, CoGroupOrder coGroupOrder) {
    super(largePipe, largeJoinFields, smallPipe, smallJoinFields, null, exact ? Mode.FILTER_EXACT : Mode.FILTER_INEXACT, null, coGroupOrder);
  }

  public BloomFilter(Pipe largePipe, Fields largeJoinFields, Pipe smallPipe, Fields smallJoinFields, boolean exact) {
    this(largePipe, largeJoinFields, smallPipe, smallJoinFields, exact, CoGroupOrder.LARGE_LHS);
  }
}
