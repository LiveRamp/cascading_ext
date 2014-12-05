package com.liveramp.cascading_ext;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.util.TupleViews;

public class FixedWidthTextScheme extends TextLine {

  private static final Logger LOG = LoggerFactory.getLogger(FixedWidthTextScheme.class);

  private final List<Integer> columnWidths;
  private final int sum;
  private final boolean isStrict;

  public FixedWidthTextScheme(Fields sourceFields, List<Integer> columnWidths, Charset charset) {
    this(sourceFields, columnWidths, charset, true);
  }

  public FixedWidthTextScheme(Fields sourceFields, List<Integer> columnWidths, Charset charset, boolean isStrict) {
    this.setSourceFields(sourceFields);
    this.setCharsetName(charset.name());

    this.isStrict = isStrict;
    this.columnWidths = columnWidths;
    this.sum = getTotalLength(columnWidths);

    ensureAllPositiveWidths(columnWidths);
    ensureWidthsMatchFields(sourceFields, columnWidths);
  }

  private String[] splitOnIndices(final String line, final List<Integer> columnWidths) {
    List<String> split = Lists.newArrayList();
    int cur = 0;
    for (Integer columnWidth : columnWidths) {
      split.add(line.substring(cur, cur + columnWidth));
      cur += columnWidth;
    }

    return split.toArray(new String[split.size()]);
  }

  @Override
  public void sourcePrepare(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) {
    super.sourcePrepare(flowProcess, sourceCall);
    sourceCall.getIncomingEntry().setTuple(TupleViews.createObjectArray());
  }

  @Override
  public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    Object[] context = sourceCall.getContext();

    while (sourceCall.getInput().next(context[0], context[1])) {
      String line = this.makeEncodedString(context);

      if (line.length() != sum) {
        String errorString = "Line is incorrectly sized. Expected size: " + sum
            + ". Line: " + line + " has length " + line.length();

        if (isStrict) {
          throw new TapException(errorString);
        } else {
          if (LOG.isWarnEnabled()) {
            LOG.warn(errorString);
          }
        }
        continue;
      }

      String[] strings = splitOnIndices(line, columnWidths);

      Tuple tuple = sourceCall.getIncomingEntry().getTuple();
      TupleViews.reset(tuple, strings);
      return true;
    }

    return false;
  }

  @Override
  public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
    throw new UnsupportedOperationException("Sinking to this tap is not allowed.");
  }

  private void ensureAllPositiveWidths(final List<Integer> columnWidths) {
    for (Integer columnWidth : columnWidths) {
      if (columnWidth <= 0) {
        throw new IllegalArgumentException("All columns must be at least one unit wide");
      }
    }
  }

  private void ensureWidthsMatchFields(final Fields sourceFields, final List<Integer> columnWidths) {
    if (sourceFields.size() != columnWidths.size()) {
      throw new IllegalArgumentException("Each column must have an associated field");
    }
  }

  private Integer getTotalLength(final List<Integer> columnWidths) {
    Integer sum = 0;
    for (Integer width : columnWidths) {
      sum += width;
    }
    return sum;
  }

}
