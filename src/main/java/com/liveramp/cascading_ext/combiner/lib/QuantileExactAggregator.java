package com.liveramp.cascading_ext.combiner.lib;

import com.clearspring.analytics.stream.quantile.QDigest;
import org.apache.hadoop.io.BytesWritable;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.liveramp.cascading_ext.Bytes;
import com.liveramp.cascading_ext.combiner.ExactAggregator;

/**
 * Memory usage scales linearly with compression factor (related to number of
 * bins used to estimate quantiles). In practice a value of 1000 seems to work
 * well.
 */
public class QuantileExactAggregator implements ExactAggregator<QDigest> {

  public static final double DEFAULT_COMPRESSION_FACTOR = 1000.0;

  private final double compressionFactor;

  public QuantileExactAggregator(double compressionFactor) {
    this.compressionFactor = compressionFactor;
  }

  public QuantileExactAggregator() {
    this(DEFAULT_COMPRESSION_FACTOR);
  }

  @Override
  public QDigest initialize() {
    return new QDigest(compressionFactor);
  }

  @Override
  public QDigest partialAggregate(QDigest aggregate, TupleEntry nextValue) {
    aggregate.offer(nextValue.getLong(0));
    return aggregate;
  }

  @Override
  public Tuple toPartialTuple(QDigest aggregate) {
    return serializeQDigest(aggregate);
  }

  @Override
  public QDigest finalAggregate(QDigest aggregate, TupleEntry partialAggregate) {
    QDigest partial = QDigest.deserialize(Bytes.getBytes((BytesWritable)partialAggregate.getObject(0)));
    return QDigest.unionOf(aggregate, partial);
  }

  @Override
  public Tuple toFinalTuple(QDigest aggregate) {
    return serializeQDigest(aggregate);
  }

  private Tuple serializeQDigest(QDigest qDigest) {
    return new Tuple(new BytesWritable(QDigest.serialize(qDigest)));
  }
}
