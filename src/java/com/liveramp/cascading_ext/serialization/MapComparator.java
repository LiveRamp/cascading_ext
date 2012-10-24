package com.liveramp.cascading_ext.serialization;

import cascading.tuple.StreamComparator;
import cascading.tuple.hadoop.io.BufferedInputStream;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

public class MapComparator<T1 extends Comparable<T1>, T2 extends Comparable<T2>> extends RawComparator
    implements StreamComparator<BufferedInputStream>, Comparator<Map<T1, T2>>, Serializable {

  private static final long serialVersionUID = 1L;

  @Override
  public int compare(Map<T1, T2> lhs, Map<T1, T2> rhs) {
    if (lhs == null) {
      if (rhs == null) {
        return 0;
      } else {
        return -1;
      }
    } else {
      if (rhs == null) {
        return 1;
      } else {
        if (lhs.size() == rhs.size()) {
          Iterator<T1> lhsItr = lhs.keySet().iterator();
          Iterator<T1> rhsItr = rhs.keySet().iterator();
          while (lhsItr.hasNext()) {
            T1 lhsKey = lhsItr.next();
            T1 rhsKey = rhsItr.next();
            int compareResult = lhsKey.compareTo(rhsKey);
            if (compareResult != 0) {
              return compareResult;
            }
          }
          return 0;
        } else {
          return lhs.size() - rhs.size();
        }
      }
    }
  }
}
