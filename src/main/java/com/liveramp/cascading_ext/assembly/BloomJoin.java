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
import cascading.pipe.joiner.Joiner;
import cascading.tuple.Fields;

/**
 * This SubAssembly behaves almost exactly like CoGroup, except that the LHS is filtered using a bloom filter
 * built from the keys on the RHS.  This means that the side with fewer keys should always be passed in as the RHS.
 * <p/>
 * Note that if there are field name conflicts on the LHS and RHS, you'll have to pass in renameFields (just like
 * with CoGroup).  Additionally, @param coGroupOrder allows tweaking of reduce performance (default is LARGE_LHS).
 * <p/>
 * Note: if this SubAssembly is used without CascadingUtil, the flow will need certain properties set.
 * See BloomJoinExampleWithoutCascadingUtil for details.
 * <p/>
 * Note: In the current implementation, using a LeftJoin joiner with LARGE_LHS or a RightJoin joiner with LARGE_RHS
 * will fall back to a regular CoGroup.
 * <p/>
 * <b>IMPORTANT:</b> one important behavior difference between BloomJoin and CoGroup is that RHS and LHS keys which are expected
 * to match in the join MUST serialize identically as well (the bloom filter is built by serializing key fields.)  If
 * normal java/hadoop types are used this should not be a problem, but comparing key types which extend each other WILL
 * cause data loss, unless custom serializers are used.  ex, LHS = (BytesWritable), RHS = (x extends BytesWritable)
 * loses data when the types serialize differently.
 */
public class BloomJoin extends BloomAssembly {

  /**
   * @param largePipe       the left hand side of the bloom operation. this will be the side that is filtered by
   *                        the bloom filter.  Usually, this should be the side with more tuples.
   * @param largeJoinFields the fields on the left hand side that will be compared with the right hand side
   * @param smallPipe       the right hand side of the bloom operation. this will be the side that is used to build the
   *                        bloom filter. Usually, this should be the side with fewer tuples.
   * @param smallJoinFields the fields on the right hand side that will be compared with the left hand side
   * @param renameFields    if doing a join and the LHS and RHS have some of the same field names, you'll need to supply
   *                        rename fields (just like with CoGroup).
   * @param joiner          allow a specific joiner to be used in the final exact join
   * @param coGroupOrder    in some cases, one pipe has a smaller cardinality but a larger number of tuples overall (some values
   *                        have high duplication.)  In this case, these values should be in the bloom filter, so should be in
   *                        "smallPipe", but also should be the LHS in the final CoGroup since they spill more frequently.
   *                        In this case, CoGroupOrder.LARGE_RHS should be used
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
