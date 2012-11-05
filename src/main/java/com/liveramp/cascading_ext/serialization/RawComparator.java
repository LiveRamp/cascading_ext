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

package com.liveramp.cascading_ext.serialization;

import cascading.tuple.StreamComparator;
import cascading.tuple.hadoop.io.BufferedInputStream;
import org.apache.hadoop.io.WritableComparator;

public abstract class RawComparator implements StreamComparator<BufferedInputStream> {
  private static final int NUM_BYTES_RESERVED_FOR_ARRAY_LENGTH = 4;

  @Override
  public int compare(BufferedInputStream lhs, BufferedInputStream rhs) {
    if (lhs == null && rhs == null) {
      return 0;
    } else if (lhs == null) {
      return -1;
    } else if (rhs == null) {
      return 1;
    }

    byte[] lhsBuffer = lhs.getBuffer();
    int lhsOff = lhs.getPosition();
    int lhsSize = readSize(lhsBuffer, lhsOff);

    byte[] rhsBuffer = rhs.getBuffer();
    int rhsOff = rhs.getPosition();
    int rhsSize = readSize(rhsBuffer, rhsOff);

    //use hadoop's lexicographical byte array ordering
    int result = compareByteArrays(
        lhsBuffer,
        lhsOff + NUM_BYTES_RESERVED_FOR_ARRAY_LENGTH,
        lhsSize,
        rhsBuffer,
        rhsOff + NUM_BYTES_RESERVED_FOR_ARRAY_LENGTH,
        rhsSize);

    lhs.skip(lhsSize + NUM_BYTES_RESERVED_FOR_ARRAY_LENGTH);
    rhs.skip(rhsSize + NUM_BYTES_RESERVED_FOR_ARRAY_LENGTH);

    return result;
  }

  public static int compareByteArrays(byte[] lhsBuffer, byte[] rhsBuffer) {
    return WritableComparator.compareBytes(lhsBuffer, 0, lhsBuffer.length, rhsBuffer, 0, rhsBuffer.length);
  }

  public static int compareByteArrays(byte[] lhsBuffer, int lhsOff, int lhsSize, byte[] rhsBuffer, int rhsOff, int rhsSize) {
    return WritableComparator.compareBytes(lhsBuffer, lhsOff, lhsSize, rhsBuffer, rhsOff, rhsSize);
  }

  protected static int readSize(byte[] buffer, int off) {

    return ((buffer[off] & 0xff) << 24) |
        ((buffer[off + 1] & 0xff) << 16) |
        ((buffer[off + 2] & 0xff) << 8) |
        ((buffer[off + 3] & 0xff));
  }
}
