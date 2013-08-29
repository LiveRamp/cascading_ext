/**
 *  Copyright 2012 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.liveramp.cascading_ext.tap;

import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.flow.FlowProcess;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.TapException;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import com.liveramp.cascading_ext.fs.TrashHelper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;

/**
 * This tap sends output to the abyss. Use it if you never want to see your
 * output again (or if there is none to speak of).
 * <p/>
 * Usage:
 * Tap sink = new NullTap();
 */
public class NullTap extends Hfs implements FlowListener {
  private static final Logger LOG = Logger.getLogger(NullTap.class);
  private final TupleEntryCollector outCollector = new NullOutputCollector();

  public NullTap() {
    this(new Fields("NullTap"));
  }

  public NullTap(Fields fields) {
    super(new TextLine(fields), new Path("/tmp/NullTap/", UUID.randomUUID().toString()).toString(), SinkMode.KEEP);
  }

  public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess, OutputCollector output) {
    return outCollector;
  }

  @Override
  public void onStarting(Flow flow) {
    // do nothing
  }

  @Override
  public void onStopping(Flow flow) {
    // do nothing
  }

  @Override
  public void onCompleted(Flow flow) {
    try {
      FileSystem fs = FileSystem.get((JobConf) flow.getConfig());
      if (fs.exists(getPath())) {
        LOG.info("Deleting NullTap path: " + getPath());
        TrashHelper.deleteUsingTrashIfEnabled(fs, getPath());
      }
    } catch (IOException e) {
      throw new TapException(e);
    }
  }

  @Override
  public boolean onThrowable(Flow flow, Throwable throwable) {
    return false;
  }

  private static class NullOutputCollector extends TupleEntryCollector implements OutputCollector<Object, Object>, Serializable {
    @Override
    public void collect(Object o, Object o1) throws IOException {
      // do nothing
    }

    @Override
    protected void collect(TupleEntry tupleEntry) throws IOException {
      // do nothing
    }
  }
}
