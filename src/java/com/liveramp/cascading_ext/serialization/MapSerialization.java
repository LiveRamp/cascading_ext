package com.liveramp.cascading_ext.serialization;

import cascading.tuple.Comparison;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import java.util.Comparator;
import java.util.Map;

public class MapSerialization implements Serialization<Map>, Comparison<Map> {

  @Override
  public Comparator<Map> getComparator(Class<Map> arg0) {
    return new MapComparator();
  }

  @Override
  public boolean accept(Class<?> klass) {
    return Map.class.isAssignableFrom(klass);
  }

  @Override
  public Deserializer<Map> getDeserializer(Class<Map> arg0) {
    return new JavaHadoopDeserializer<Map>();
  }

  @Override
  public Serializer<Map> getSerializer(Class<Map> arg0) {
    return new JavaHadoopSerializer<Map>();
  }

  public Class getInstanceClass() {
    return Map.class;
  }

  public static byte[] intToByteArray(int value) {
    byte[] b = new byte[4];

    for (int i = 0; i < 4; i++) {
      int offset = (b.length - 1 - i) * 8;
      b[i] = (byte) ((value >>> offset) & 0xFF);
    }

    return b;
  }
}
