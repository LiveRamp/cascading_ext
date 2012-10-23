package com.liveramp.cascading_ext;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.io.BytesWritable;

import java.nio.ByteBuffer;

public class Bytes {

  /**
   * If the given ByteBuffer wraps completely its underlying byte array, return the underlying
   * byte array (no copy). Otherwise, return a deep copy of the range represented by the given
   * ByteBuffer.
   *
   * @param byteBuffer
   * @return
   */
  public static byte[] byteBufferToByteArray(ByteBuffer byteBuffer) {
    if (wrapsFullArray(byteBuffer)) {
      return byteBuffer.array();
    } else {
      return byteBufferDeepCopy(byteBuffer);
    }
  }

  public static boolean wrapsFullArray(ByteBuffer byteBuffer) {
    return byteBuffer.hasArray()
            && byteBuffer.arrayOffset() == 0
            && byteBuffer.position() == 0
            && byteBuffer.remaining() == byteBuffer.capacity();
  }

  /**
   * Always return a byte array that is a deep copy of the range represented
   * by the given ByteBuffer.
   *
   * @param byteBuffer
   * @return
   */
  public static byte[] byteBufferDeepCopy(ByteBuffer byteBuffer) {
    byte[] result = new byte[byteBuffer.remaining()];
    System.arraycopy(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), result, 0, byteBuffer.remaining());
    return result;
  }

  // Stuff i've moved from Rap

  public static int compareBytes(byte[] b1, byte[] b2) {
    if (b1.length < b2.length)
      return -1;
    if (b1.length > b2.length)
      return 1;
    for (int i = 0; i < b1.length; i++) {
      if (b1[i] < b2[i])
        return -1;
      if (b1[i] > b2[i])
        return 1;
    }
    return 0;
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
    return getBytes((BytesWritable) tupleEntry.get(fieldName));
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
    return getBytes((BytesWritable) tuple.get(fieldIndex));
  }
}
