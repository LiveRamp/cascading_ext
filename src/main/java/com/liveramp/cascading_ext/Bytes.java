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

import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.liveramp.commons.util.BytesUtils;
/**
 * Collection of methods for safely converting between byte[], BytesWritable and ByteBuffer. Some
 * byte[] methods delegate to Guava's UnsignedBytes
 */
public class Bytes {
  public static BytesWritable byteBufferToBytesWritable(ByteBuffer buffer){
    return new BytesWritable(BytesUtils.byteBufferToByteArray(buffer));
  }

  public static ByteBuffer bytesWriteableToByteBuffer(BytesWritable writable){
    return ByteBuffer.wrap(getBytes(writable));
  }

  public static byte[] getBytes(BytesWritable bw) {
    if (bw.getCapacity() == bw.getLength()) {
      return bw.getBytes();
    } else {
      return copyBytes(bw);
    }
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
