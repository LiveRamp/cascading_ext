package com.liveramp.cascading_ext.combiner;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.liveramp.commons.util.MemoryUsageEstimator;

import java.io.Serializable;

public class CombinerDefinition<T> implements Serializable {

  private final PartialAggregator<T> partialAggregator;
  private final FinalAggregator<T> finalAggregator;
  private final Fields groupFields;
  private final Fields inputFields;
  private final Fields intermediateFields;
  private final Fields outputFields;
  private final int id;
  private final String name;
  private final int limit;
  private final long memoryLimit;
  private final MemoryUsageEstimator<Tuple> keySizeEstimator;
  private final MemoryUsageEstimator<T> valueSizeEstimator;
  private final boolean strict;
  private final Evictor<T> evictor;
  private final boolean keepNullGroups;

  protected CombinerDefinition(
      PartialAggregator<T> partialAggregator,
      FinalAggregator<T> finalAggregator,
      Fields groupFields,
      Fields inputFields,
      Fields intermediateFields,
      Fields outputFields,
      String name,
      int limit,
      long memoryLimit,
      MemoryUsageEstimator<Tuple> keySizeEstimator,
      MemoryUsageEstimator<T> valueSizeEstimator,
      boolean strict,
      Evictor<T> evictor,
      boolean keepNullGroups) {

    this.partialAggregator = partialAggregator;
    this.finalAggregator = finalAggregator;
    this.groupFields = groupFields;
    this.inputFields = inputFields;
    this.intermediateFields = intermediateFields;
    this.outputFields = outputFields;
    this.name = name != null ? name : makeName();
    this.id = this.name.hashCode();
    this.keepNullGroups = keepNullGroups;
    this.limit = limit;
    this.memoryLimit = memoryLimit;
    this.keySizeEstimator = keySizeEstimator;
    this.valueSizeEstimator = valueSizeEstimator;
    this.strict = strict;
    this.evictor = evictor;
    if (limit <= 0 && memoryLimit <= 0) {
      throw new IllegalArgumentException("Either size limit or memory limit must be specified");
    }
  }

  private String makeName() {
    String name = "";
    name += partialAggregator != null ? partialAggregator.getClass().getSimpleName() : "NoPartial";
    name += finalAggregator != null ? finalAggregator.getClass().getSimpleName() : "NoFinal";
    name += groupFields != null ? groupFields.toString() : "NoGroupFields";
    return name;
  }

  public PartialAggregator<T> getPartialAggregator() {
    return partialAggregator;
  }

  public FinalAggregator<T> getFinalAggregator() {
    return finalAggregator;
  }

  public Fields getGroupFields() {
    return groupFields;
  }

  public Fields getInputFields() {
    return inputFields;
  }

  public Fields getIntermediateFields() {
    return intermediateFields;
  }

  public Fields getOutputFields() {
    return outputFields;
  }

  public Integer getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public int getLimit() {
    return limit;
  }

  public long getMemoryLimit() {
    return memoryLimit;
  }

  public MemoryUsageEstimator<Tuple> getKeySizeEstimator() {
    return keySizeEstimator;
  }

  public MemoryUsageEstimator<T> getValueSizeEstimator() {
    return valueSizeEstimator;
  }

  public boolean isStrict() {
    return strict;
  }

  public Evictor<T> getEvictor() {
    return evictor;
  }

  public boolean shouldKeepNullGroups() {
    return keepNullGroups;
  }
}
