package com.liveramp.cascading_ext.combiner;

import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class StrictCombiner<T> extends Combiner<T> {

  public StrictCombiner(Pipe pipe,
                        ExactAggregator<T> aggregator,
                        Fields groupFields,
                        Fields inputFields,
                        Fields outputFields) {
    super(pipe, aggregator, groupFields, inputFields, outputFields, true);
  }

  public StrictCombiner(Pipe pipe,
                        ExactAggregator<T> aggregator,
                        Fields groupFields,
                        Fields inputFields,
                        Fields intermediateFields,
                        Fields outputFields) {
    super(pipe, aggregator, groupFields, inputFields, intermediateFields, outputFields, true);
  }

  public StrictCombiner(Pipe pipe,
                        ExactAggregator<T> aggregator,
                        Fields groupFields,
                        Fields inputFields,
                        Fields intermediateFields,
                        Fields outputFields,
                        int limit) {
    super(pipe, aggregator, groupFields, inputFields, intermediateFields, outputFields, limit, true);
  }

  public StrictCombiner(Pipe[] pipes,
                        ExactAggregator<T> aggregator,
                        Fields groupFields,
                        Fields inputFields,
                        Fields outputFields) {
    super(pipes, aggregator, groupFields, inputFields, outputFields, true);
  }

  public StrictCombiner(Pipe[] pipes,
                        ExactAggregator<T> aggregator,
                        Fields groupFields,
                        Fields inputFields,
                        Fields intermediateFields,
                        Fields outputFields) {
    super(pipes, aggregator, groupFields, inputFields, intermediateFields, outputFields, true);
  }

  public StrictCombiner(Pipe[] pipes,
                        ExactAggregator<T> aggregator,
                        Fields groupFields,
                        Fields inputFields,
                        Fields intermediateFields,
                        Fields outputFields,
                        int limit) {
    super(pipes, aggregator, groupFields, inputFields, intermediateFields, outputFields, limit, true);
  }

  public static <T> StrictCombiner<T> assembly(Pipe pipe,
                                               ExactAggregator<T> aggregator,
                                               Fields groupFields,
                                               Fields inputFields,
                                               Fields outputFields) {
    return new StrictCombiner<T>(pipe, aggregator, groupFields, inputFields, outputFields);
  }

  public static <T> StrictCombiner<T> assembly(Pipe pipe,
                                               ExactAggregator<T> aggregator,
                                               Fields groupFields,
                                               Fields inputFields,
                                               Fields intermediateFields,
                                               Fields outputFields) {
    return new StrictCombiner<T>(pipe, aggregator, groupFields, inputFields, intermediateFields, outputFields);
  }

  public static <T> StrictCombiner<T> assembly(Pipe pipe,
                                               ExactAggregator<T> aggregator,
                                               Fields groupFields,
                                               Fields inputFields,
                                               Fields intermediateFields,
                                               Fields outputFields,
                                               int limit) {
    return new StrictCombiner<T>(pipe, aggregator, groupFields, inputFields, intermediateFields, outputFields, limit);
  }

  public static <T> StrictCombiner<T> assembly(Pipe[] pipes,
                                               ExactAggregator<T> aggregator,
                                               Fields groupFields,
                                               Fields inputFields,
                                               Fields outputFields) {
    return new StrictCombiner<T>(pipes, aggregator, groupFields, inputFields, outputFields);
  }

  public static <T> StrictCombiner<T> assembly(Pipe[] pipes,
                                               ExactAggregator<T> aggregator,
                                               Fields groupFields,
                                               Fields inputFields,
                                               Fields intermediateFields,
                                               Fields outputFields) {
    return new StrictCombiner<T>(pipes, aggregator, groupFields, inputFields, intermediateFields, outputFields);
  }

  public static <T> StrictCombiner<T> assembly(Pipe[] pipes,
                                               ExactAggregator<T> aggregator,
                                               Fields groupFields,
                                               Fields inputFields,
                                               Fields intermediateFields,
                                               Fields outputFields,
                                               int limit) {
    return new StrictCombiner<T>(pipes, aggregator, groupFields, inputFields, intermediateFields, outputFields, limit);
  }
}
