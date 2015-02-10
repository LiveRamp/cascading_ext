package com.liveramp.cascading_ext.assembly;

import cascading.operation.Aggregator;
import cascading.operation.Buffer;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import com.liveramp.cascading_ext.operation.AggregatorStats;
import com.liveramp.cascading_ext.operation.BufferStats;
import com.liveramp.cascading_ext.util.OperationStatsUtils;

public class EveryStats extends SubAssembly {

  private static Buffer decorateBuffer(Buffer buffer) {
    return new BufferStats(OperationStatsUtils.getStackPosition(2), buffer);
  }

  private static Aggregator decorateAggregator(Aggregator aggregator) {
    return new AggregatorStats(OperationStatsUtils.getStackPosition(2), aggregator);
  }

  public EveryStats(Pipe pipe, Aggregator aggregator) {
    super(pipe);
    setTails(new Every(pipe, decorateAggregator(aggregator)));
  }

  public EveryStats(Pipe pipe, Fields comparables, Aggregator aggregator) {
    super(pipe);
    setTails(new Every(pipe, comparables, decorateAggregator(aggregator)));
  }

  public EveryStats(Pipe pipe, Fields comparables, Aggregator aggregator, Fields comparables2) {
    super(pipe);
    setTails(new Every(pipe, comparables, decorateAggregator(aggregator), comparables2));
  }

  public EveryStats(Pipe pipe, Aggregator aggregator, Fields comparables) {
    super(pipe);
    setTails(new Every(pipe, decorateAggregator(aggregator), comparables));
  }

  public EveryStats(Pipe pipe, Buffer buffer) {
    super(pipe);
    setTails(new Every(pipe, decorateBuffer(buffer)));
  }

  public EveryStats(Pipe pipe, Fields comparables, Buffer buffer) {
    super(pipe);
    setTails(new Every(pipe, comparables, decorateBuffer(buffer)));
  }

  public EveryStats(Pipe pipe, Fields comparables, Buffer buffer, Fields comparables2) {
    super(pipe);
    setTails(new Every(pipe, comparables, decorateBuffer(buffer), comparables2));
  }

  public EveryStats(Pipe pipe, Buffer buffer, Fields comparables) {
    super(pipe);
    setTails(new Every(pipe, decorateBuffer(buffer), comparables));
  }
}
