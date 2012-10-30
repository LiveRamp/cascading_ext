package com.liveramp.cascading_ext.serialization;

import org.apache.hadoop.io.serializer.Serializer;

import java.io.*;

/**
 * Generic Hadoop serializer that uses Java serialization under the hood.
 */
public class JavaHadoopSerializer<T> implements Serializer<T> {
  private DataOutputStream dataOut;

  @Override
  public void close() throws IOException {
    dataOut.flush();
    dataOut.close();
  }

  @Override
  public void open(OutputStream stream) throws IOException {
    dataOut = new DataOutputStream(stream);
  }

  @Override
  public void serialize(T obj) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream tempOut = new ObjectOutputStream(bos);

    try {
      tempOut.writeObject(obj);
      tempOut.flush();

      byte[] bosBytes = bos.toByteArray();
      dataOut.writeInt(bosBytes.length);
      dataOut.write(bosBytes);
    } finally {
      bos.flush();
      bos.close();
      tempOut.flush();
      tempOut.close();
    }
  }
}
