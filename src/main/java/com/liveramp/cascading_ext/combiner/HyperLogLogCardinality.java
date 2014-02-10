package com.liveramp.cascading_ext.combiner;

import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import com.clearspring.analytics.stream.cardinality.ICardinality;

public class HyperLogLogCardinality extends StrictCombiner<ICardinality> {

  /**
   * Creates HyperLogLog objects for each distinct set of <i>inputFields</i>,
   * as grouped by <i>groupFields</i>. These objects are placed in the field "hll_object".
   *
   * @param pipe
   * @param groupFields
   * @param inputFields
   */

  public HyperLogLogCardinality(Pipe pipe, Fields groupFields, Fields inputFields) {
    this(pipe, groupFields, inputFields, "hll_object", "cardinality");
  }

  /**
   * Creates HyperLogLog objects for each distinct set of <i>inputFields</i>, as grouped by <i>groupFields</i>.
   * Specify a field name for these in <i>hllObjectFieldName</i>, and another for cardinality in <i>cardinalityFieldName</i>.
   *
   * @param pipe
   * @param groupFields
   * @param inputFields
   * @param hllObjectFieldName
   * @param cardinalityFieldName
   */

  public HyperLogLogCardinality(Pipe pipe, Fields groupFields, Fields inputFields, String hllObjectFieldName, String cardinalityFieldName) {
    this(new Pipe[]{pipe}, groupFields, inputFields, hllObjectFieldName, cardinalityFieldName, HyperLogLogExactAggregator.DEFAULT_PRECISION, DEFAULT_LIMIT);
  }


  /**
   * Creates HyperLogLog objects for each distinct set of <i>inputFields</i>, as grouped by <i>groupFields</i>.
   * Specify a field name hll objects in <i>hllObjectFieldName</i>, and for cardinality in <i>cardinalityFieldName</i>.
   * <i>precision</i> is the p value in HyperLogLogPlus and defaults to 16.
   * <i>limit</i> is for the LRUHashMap underlying the combiner.
   *
   * @param pipes
   * @param groupFields
   * @param inputFields
   * @param hllObjectFieldName
   * @param cardinalityFieldName
   * @param precision
   * @param limit
   */

  public HyperLogLogCardinality(Pipe[] pipes, Fields groupFields, Fields inputFields, String hllObjectFieldName, String cardinalityFieldName, int precision, int limit) {
    super(pipes,
        new HyperLogLogExactAggregator(precision, HyperLogLogExactAggregator.OutputOption.CARDINALITY_AND_HLL_OBJECT), 
        groupFields, 
        inputFields, 
        new Fields(hllObjectFieldName), 
        new Fields(hllObjectFieldName, cardinalityFieldName), limit);
  }
}


