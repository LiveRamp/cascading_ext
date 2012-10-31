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

import org.apache.hadoop.io.serializer.Deserializer;

import java.io.*;

public class JavaHadoopDeserializer<T> implements Deserializer<T> {
  private DataInputStream dataIn;
  private byte[] BUFFER = new byte[1024 * 30];

  @Override
  public void open(InputStream inputStream) throws IOException {
    dataIn = new DataInputStream(inputStream);
  }

  @Override
  public T deserialize(T deserializedObject) throws IOException {
    ObjectInputStream tempIn = null;

    try {
      byte[] buffer = BUFFER;
      int size = dataIn.readInt();

      if (size > BUFFER.length) {
        buffer = new byte[size];
      }

      int curIndex = 0;
      int bytesRead;

      while ((bytesRead = dataIn.read(buffer, curIndex, size - curIndex)) != -1) {
        curIndex += bytesRead;

        if (curIndex == size)
          break;
      }

      tempIn = new ObjectInputStream(new ByteArrayInputStream(buffer));
      deserializedObject = (T) tempIn.readObject();
    } catch (ClassNotFoundException e) {
      // Should never happen
      throw new RuntimeException(e);
    } finally {
      if (tempIn != null) {
        tempIn.close();
      }
    }

    return deserializedObject;
  }

  @Override
  public void close() throws IOException {
    dataIn.close();
  }
}
