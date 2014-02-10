package com.liveramp.cascading_ext.combiner;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.ArrayList;

public class CombinerUtils {

  private CombinerUtils() {

  }

  /**
   * Sets values for selectorFields from the inputTupleEntry into the outputTupleEntry.
   * We do this instead of using inputTupleEntry.selectTuple() so that it doesn't
   * create a new Tuple instance.
   * Caches selectorFieldsPos the first time for optimization.
   *
   * @param outputTupleEntry
   * @param selectorFields
   * @param inputTupleEntry
   */
  public static void setTupleEntry(TupleEntry outputTupleEntry,
      ArrayList<Integer> selectorFieldsPos,
      Fields selectorFields,
      TupleEntry inputTupleEntry) {
    if (selectorFieldsPos.size() == 0) {
      for (int i = 0; i < selectorFields.size(); i++) {
        selectorFieldsPos.add(inputTupleEntry.getFields().getPos(selectorFields.get(i)));
      }
    }

    for (int i = 0; i < selectorFieldsPos.size(); i++) {
      outputTupleEntry.set(i, inputTupleEntry.getObject(selectorFieldsPos.get(i)));
    }
  }
}
