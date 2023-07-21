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
      outputTupleEntry.setRaw(i, inputTupleEntry.getObject(selectorFieldsPos.get(i)));
    }
  }
}
