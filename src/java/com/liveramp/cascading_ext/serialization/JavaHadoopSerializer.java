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
