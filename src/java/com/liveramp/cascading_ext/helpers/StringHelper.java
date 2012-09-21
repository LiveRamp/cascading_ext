package com.liveramp.cascading_ext.helpers;

import java.util.Iterator;

/**
 * @author eddie
 */
public class StringHelper {
  private StringHelper() {}

  public static <T> String join(Iterable<T> iterable, String sep) {
    StringBuilder ret = new StringBuilder();
    Iterator<T> iter = iterable.iterator();
    while (iter.hasNext()) {
      T obj = iter.next();
      ret.append(obj.toString());
      if (iter.hasNext()) {
        ret.append(sep);
      }
    }
    return ret.toString();
  }
}
