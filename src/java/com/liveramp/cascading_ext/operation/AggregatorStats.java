package com.liveramp.cascading_ext.operation;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.ConcreteCall;
import cascading.stats.FlowStats;
import com.liveramp.cascading_ext.operation.forwarding.ForwardingAggregator;

// An AggregatorStats instance decorates an Aggregator instance and
// automatically maintains input/output records counters in addition to
// providing the functionality of the wrapped object.
public class AggregatorStats extends ForwardingAggregator {

  public static final String INPUT_RECORDS_COUNTER_NAME = "Input records";
  public static final String START_OUTPUT_RECORDS_COUNTER_NAME = "Start output records";
  public static final String AGGREGATION_OUTPUT_RECORDS_COUNTER_NAME = "Aggregation output records";
  public static final String COMPLETION_OUTPUT_RECORDS_COUNTER_NAME = "Completion output records";
  public static final String TOTAL_OUTPUT_RECORDS_COUNTER_NAME = "Total output records";

  private final String nameInputRecords;
  private final String nameStartOutputRecords;
  private final String nameAggregationOutputRecords;
  private final String nameCompletionOutputRecords;
  private final String nameTotalOutputRecords;

  public AggregatorStats(Aggregator aggregator) {
    super(aggregator);
    String className = aggregator.getClass().getSimpleName();
    this.nameInputRecords = className + " - " + INPUT_RECORDS_COUNTER_NAME;
    this.nameStartOutputRecords = className + " - " + START_OUTPUT_RECORDS_COUNTER_NAME;
    this.nameAggregationOutputRecords = className + " - " + AGGREGATION_OUTPUT_RECORDS_COUNTER_NAME;
    this.nameCompletionOutputRecords = className + " - " + COMPLETION_OUTPUT_RECORDS_COUNTER_NAME;
    this.nameTotalOutputRecords = className + " - " + TOTAL_OUTPUT_RECORDS_COUNTER_NAME;
  }

  public AggregatorStats(Aggregator aggregator, String name) {
    super(aggregator);
    String className = aggregator.getClass().getSimpleName();
    this.nameInputRecords = className + " - " + name + " - " + INPUT_RECORDS_COUNTER_NAME;
    this.nameStartOutputRecords = className + " - " + name + " - " + START_OUTPUT_RECORDS_COUNTER_NAME;
    this.nameAggregationOutputRecords = className + " - " + name + " - " + AGGREGATION_OUTPUT_RECORDS_COUNTER_NAME;
    this.nameCompletionOutputRecords = className + " - " + name + " - " + COMPLETION_OUTPUT_RECORDS_COUNTER_NAME;
    this.nameTotalOutputRecords = className + " - " + name + " - " + TOTAL_OUTPUT_RECORDS_COUNTER_NAME;
  }

  @Override
  public void start(FlowProcess process, AggregatorCall call) {
    CascadingOperationStatsUtils.TupleEntryCollectorCounter outputCollectorCounter = new CascadingOperationStatsUtils.TupleEntryCollectorCounter(call.getOutputCollector());
    super.start(process, CascadingOperationStatsUtils.copyConcreteCallAndSetOutputCollector((ConcreteCall) call, outputCollectorCounter));
    if (outputCollectorCounter.getCount() > 0) {
      process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameStartOutputRecords, outputCollectorCounter.getCount());
      process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameTotalOutputRecords, outputCollectorCounter.getCount());
    }
  }

  @Override
  public void aggregate(FlowProcess process, AggregatorCall call) {
    CascadingOperationStatsUtils.TupleEntryCollectorCounter outputCollectorCounter = new CascadingOperationStatsUtils.TupleEntryCollectorCounter(call.getOutputCollector());
    super.aggregate(process, CascadingOperationStatsUtils.copyConcreteCallAndSetOutputCollector((ConcreteCall) call, outputCollectorCounter));
    process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameInputRecords, 1);
    if (outputCollectorCounter.getCount() > 0) {
      process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameAggregationOutputRecords, outputCollectorCounter.getCount());
      process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameTotalOutputRecords, outputCollectorCounter.getCount());
    }
  }

  @Override
  public void complete(FlowProcess process, AggregatorCall call) {
    CascadingOperationStatsUtils.TupleEntryCollectorCounter outputCollectorCounter = new CascadingOperationStatsUtils.TupleEntryCollectorCounter(call.getOutputCollector());
    super.start(process, CascadingOperationStatsUtils.copyConcreteCallAndSetOutputCollector((ConcreteCall) call, outputCollectorCounter));
    if (outputCollectorCounter.getCount() > 0) {
      process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameCompletionOutputRecords, outputCollectorCounter.getCount());
      process.increment(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameTotalOutputRecords, outputCollectorCounter.getCount());
    }
  }

  public long getInputRecords(FlowStats flowStats) {
    return flowStats.getCounterValue(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameInputRecords);
  }

  public long getStartOutputRecords(FlowStats flowStats) {
    return flowStats.getCounterValue(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameStartOutputRecords);
  }

  public long getAggregationOutputRecords(FlowStats flowStats) {
    return flowStats.getCounterValue(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameAggregationOutputRecords);
  }

  public long getCompletionOutputRecords(FlowStats flowStats) {
    return flowStats.getCounterValue(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameCompletionOutputRecords);
  }

  public long getTotalOutputRecords(FlowStats flowStats) {
    return flowStats.getCounterValue(CascadingOperationStatsUtils.COUNTER_CATEGORY, nameTotalOutputRecords);
  }
}
