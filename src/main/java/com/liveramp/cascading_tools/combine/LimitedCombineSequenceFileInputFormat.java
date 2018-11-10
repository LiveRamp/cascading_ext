package com.liveramp.cascading_tools.combine;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class LimitedCombineSequenceFileInputFormat<K,V>
    extends LimitedCombineFileInputFormatMapred<K,V> {
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public RecordReader<K,V> getRecordReader(InputSplit split, JobConf conf,
                                           Reporter reporter) throws IOException {
    return new CombineFileRecordReader(conf, (CombineFileSplit)split, reporter,
        SequenceFileRecordReaderWrapper.class);
  }

  /**
   * A record reader that may be passed to <code>CombineFileRecordReader</code>
   * so that it can be used in a <code>CombineFileInputFormat</code>-equivalent
   * for <code>SequenceFileInputFormat</code>.
   *
   * @see CombineFileRecordReader
   * @see CombineFileInputFormat
   * @see SequenceFileInputFormat
   */
  private static class SequenceFileRecordReaderWrapper<K,V>
      extends CombineFileRecordReaderWrapper<K,V> {
    // this constructor signature is required by CombineFileRecordReader
    public SequenceFileRecordReaderWrapper(CombineFileSplit split,
                                           Configuration conf, Reporter reporter, Integer idx) throws IOException {
      super(new SequenceFileInputFormat<K,V>(), split, conf, reporter, idx);
    }
  }
}