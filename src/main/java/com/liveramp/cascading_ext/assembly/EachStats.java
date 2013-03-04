package com.liveramp.cascading_ext.assembly;

import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import com.liveramp.cascading_ext.operation.FilterStats;
import com.liveramp.cascading_ext.operation.FunctionStats;
import com.liveramp.cascading_ext.util.OperationStatsUtils;

public class EachStats extends SubAssembly {

  private static Function decorateFunction(Function function) {
    return new FunctionStats(OperationStatsUtils.getStackPosition(2)
        + " - " + function.getClass().getSimpleName(), function);
  }

  private static Filter decorateFilter(Filter filter) {
    return new FilterStats(OperationStatsUtils.getStackPosition(2)
        + " - " + filter.getClass().getSimpleName(), filter);
  }

  public EachStats(String s, Function function) {
    setTails(new Each(s, decorateFunction(function)));
  }

  public EachStats(String s, Fields comparables, Function function) {
    setTails(new Each(s, comparables, decorateFunction(function)));
  }

  public EachStats(String s, Fields comparables, Function function, Fields comparables2) {
    setTails(new Each(s, comparables, decorateFunction(function), comparables2));
  }

  public EachStats(String s, Function function, Fields comparables) {
    setTails(new Each(s, decorateFunction(function), comparables));
  }

  public EachStats(Pipe pipe, Function function) {
    setTails(new Each(pipe, decorateFunction(function)));
  }

  public EachStats(Pipe pipe, Fields comparables, Function function) {
    setTails(new Each(pipe, comparables, decorateFunction(function)));
  }

  public EachStats(Pipe pipe, Fields comparables, Function function, Fields comparables2) {
    setTails(new Each(pipe, comparables, decorateFunction(function), comparables2));
  }

  public EachStats(Pipe pipe, Function function, Fields comparables) {
    setTails(new Each(pipe, decorateFunction(function), comparables));
  }

  public EachStats(String s, Filter filter) {
    setTails(new Each(s, decorateFilter(filter)));
  }

  public EachStats(String s, Fields comparables, Filter filter) {
    setTails(new Each(s, comparables, decorateFilter(filter)));
  }

  public EachStats(Pipe pipe, Filter filter) {
    setTails(new Each(pipe, decorateFilter(filter)));
  }

  public EachStats(Pipe pipe, Fields comparables, Filter filter) {
    setTails(new Each(pipe, comparables, decorateFilter(filter)));
  }
}
