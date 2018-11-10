package com.liveramp.cascading_tools.combine;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.rapleaf.cascading_ext.tap.bucket2.batch.BatchBucketTapProps;

import static com.rapleaf.cascading_ext.tap.bucket2.batch.BatchBucketTap.DEFAULT_MAX_SPLIT_SIZE;
import static com.rapleaf.cascading_ext.tap.bucket2.batch.BatchBucketTap.DEFAULT_MIN_SPLIT_SIZE;

public class BatchedSequenceFileScheme extends cascading.scheme.hadoop.SequenceFile {

  public static final long DEFAULT_BLOCK_SIZE = 255L * 1024 * 1024;
  public static final long DEFAULT_MIN_SPLIT_SIZE = DEFAULT_BLOCK_SIZE;
  public static final long DEFAULT_MAX_SPLIT_SIZE = 3L * DEFAULT_BLOCK_SIZE;

  public static final String MAPRED_MAX_SPLIT_SIZE_PROPERTY_NAME = "mapred.max.split.size";
  public static final String MAPRED_MIN_SPLIT_SIZE_PROPERTY_NAME = "mapred.min.split.size";

  public BatchedSequenceFileScheme(Fields fields) {
    super(fields);
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

    conf.setLong(MAPRED_MIN_SPLIT_SIZE_PROPERTY_NAME, DEFAULT_MIN_SPLIT_SIZE);
    conf.setLong(MAPRED_MAX_SPLIT_SIZE_PROPERTY_NAME, DEFAULT_MAX_SPLIT_SIZE);

    conf.setInputFormat(LimitedCombineSequenceFileInputFormat.class);
  }

}
