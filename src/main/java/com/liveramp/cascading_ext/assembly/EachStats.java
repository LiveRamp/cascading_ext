package com.liveramp.cascading_ext.assembly;

import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import com.liveramp.cascading_ext.operation.FilterStats;
import com.liveramp.cascading_ext.operation.FunctionStats;
import com.liveramp.cascading_ext.util.OperationStatsUtils;

public class EachStats extends SubAssembly {

  public EachStats(String s, Function function) {
    setTails(new Each(s, new FunctionStats(OperationStatsUtils.getStackPosition(1)
        + " - " + function.getClass().getSimpleName(), function)));
  }

  public EachStats(Pipe pipe, Filter filter) {
    setTails(new Each(pipe, new FilterStats(OperationStatsUtils.getStackPosition(1)
        + " - " + filter.getClass().getSimpleName(), filter)));
  }
}
