package com.liveramp.cascading_ext.flow;

import java.util.Collections;

import com.google.common.collect.Lists;
import com.twitter.maple.tap.MemorySourceTap;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.tap.NullTap;

import static org.junit.Assert.assertEquals;

public class TestLoggingFlow extends BaseTestCase {

  @Test
  public void testMem() throws Exception {

    CascadingUtil.get().setDefaultProperty(CascadingUtil.MAX_TASK_MEMORY, 1324*1024l*1024l);

    CascadingUtil.get().getFlowConnector().connect(
        new MemorySourceTap(Lists.<Tuple>newArrayList(), new Fields("blank")),
        new NullTap(),
        new Pipe("pipe")
    );

    try {
      CascadingUtil.get().getFlowConnector(Collections.<Object, Object>singletonMap(JobConf.MAPRED_TASK_JAVA_OPTS, "-MyOwnThing")).connect(
          new MemorySourceTap(Lists.<Tuple>newArrayList(), new Fields("blank")),
          new NullTap(),
          new Pipe("pipe")
      );
    }catch(Exception e){
      assertEquals("Cannot set property mapred.child.java.opts without specifying a max heap size!", e.getCause().getMessage());
    }

    try {
      CascadingUtil.get().getFlowConnector(Collections.<Object, Object>singletonMap(JobConf.MAPRED_TASK_JAVA_OPTS, "-Xmx2048m")).connect(
          new MemorySourceTap(Lists.<Tuple>newArrayList(), new Fields("blank")),
          new NullTap(),
          new Pipe("pipe")
      );
    }catch(Exception e){
      assertEquals("Job configured memory 2147483648 violates global max 1388314624!", e.getCause().getMessage());
    }



  }
}