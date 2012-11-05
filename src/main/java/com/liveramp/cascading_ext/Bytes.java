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

package com.liveramp.cascading_ext;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.primitives.UnsignedBytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TBaseHelper;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * Collection of methods for safely converting between byte[], BytesWritable and ByteBuffer.  Some ByteBuffer
 * related methods delegate to TBaseHelper, some byte[] methods to Guava's UnsignedBytes
 */
public class Bytes {
  private static final Comparator<byte[]> BYTES_COMPARATOR = UnsignedBytes.lexicographicalComparator();

  /**
   * If the given ByteBuffer wraps completely its underlying byte array, return the underlying
   * byte array (no copy). Otherwise, return a deep copy of the range represented by the given
   * ByteBuffer.
   *
   * @param byteBuffer
   */
  public static byte[] byteBufferToByteArray(ByteBuffer byteBuffer) {
    return TBaseHelper.byteBufferToByteArray(byteBuffer);
  }

  public static boolean wrapsFullArray(ByteBuffer byteBuffer) {
    return TBaseHelper.wrapsFullArray(byteBuffer);
  }

  /**
   * Always return a byte array that is a deep copy of the range represented
   * by the given ByteBuffer.
   *
   * @param byteBuffer
   * @return
   */
  public static byte[] byteBufferDeepCopy(ByteBuffer byteBuffer) {
    byte[] target = new byte[byteBuffer.remaining()];
    TBaseHelper.byteBufferToByteArray(byteBuffer, target, 0);
    return target;
  }

  public static int compareBytes(byte[] b1, byte[] b2) {
    return BYTES_COMPARATOR.compare(b1, b2);
  }

  public static byte[] getBytes(BytesWritable bw) {
    if (bw.getCapacity() == bw.getLength()) {
      return bw.getBytes();
    }
    byte[] ret = new byte[bw.getLength()];
    System.arraycopy(bw.getBytes(), 0, ret, 0, bw.getLength());
    return ret;
  }

  public static byte[] copyBytes(BytesWritable bw) {
    byte[] ret = new byte[bw.getLength()];
    System.arraycopy(bw.getBytes(), 0, ret, 0, bw.getLength());
    return ret;
  }

  /**
   * Safely gets a BytesWritable field from a TupleEntry. It automatically deals
   * with padded BytesWritable.
   *
   * @param tupleEntry
   * @param fieldIndex the index of the field to be retrieved from the tuple entry
   * @return the field bytes
   */
  public static byte[] getBytes(TupleEntry tupleEntry, int fieldIndex) {
    return getBytes(tupleEntry.getTuple(), fieldIndex);
  }

  /**
   * Safely gets a BytesWritable field from a TupleEntry. It automatically deals
   * with padded BytesWritable.
   *
   * @param tupleEntry
   * @param fieldName  the tuple index of the field to be retrieved
   * @return the field bytes
   */
  public static byte[] getBytes(TupleEntry tupleEntry, String fieldName) {
    return getBytes((BytesWritable) tupleEntry.getObject(fieldName));
  }

  /**
   * Safely gets a BytesWritable field from a TupleEntry. It automatically deals
   * with padded BytesWritable.
   *
   * @param tuple
   * @param fieldIndex the index of the field to be retrieved from the tuple
   * @return the field bytes
   */
  public static byte[] getBytes(Tuple tuple, int fieldIndex) {
    return getBytes((BytesWritable) tuple.getObject(fieldIndex));
  }
}
