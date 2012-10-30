package com.liveramp.cascading_ext.joiner;

import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.tap.TapHelper;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestLimitJoin extends BaseTestCase {

  private Tap inputLhs;
  private Tap inputRhs;

  @Before
  public void setUp() throws IOException {

    inputLhs = new Hfs(new SequenceFile(new Fields("a", "b")), getTestRoot()+"/lhs");
    inputRhs = new Hfs(new SequenceFile(new Fields("c", "d")), getTestRoot()+"/rhs");

    TapHelper.writeToTap(inputLhs,
        new Tuple("1", "A"),
        new Tuple("1", "B"),
        new Tuple("1", "C"),
        new Tuple("2", "D"),
        new Tuple("2", "H"),
        new Tuple("2", "I"));

    TapHelper.writeToTap(inputRhs,
        new Tuple("1", "E"),
        new Tuple("2", "F"),
        new Tuple("2", "G"));
  }

  @Test
  public void checkLimits() throws IOException {
    Tap output = new Hfs(new SequenceFile(new Fields("a", "b", "d")), getTestRoot()+"/output");

    Pipe lhs = new Pipe("lhs");

    Pipe rhs = new Pipe("rhs");

    Pipe joined = new CoGroup(lhs, new Fields("a"),
        rhs, new Fields("c"), new LimitJoin(new long[]{2, 1}));

    Map<String, Tap> sources = new HashMap<String, Tap>();
    sources.put("lhs", inputLhs);
    sources.put("rhs", inputRhs);

    CascadingUtil.get().getFlowConnector().connect(sources, output, joined).complete();

    List<Tuple> results = TapHelper.getAllTuples(output);
    Assert.assertEquals(results.get(0), new Tuple("1", "A", "E"));
    Assert.assertEquals(results.get(1), new Tuple("1", "B", "E"));
    Assert.assertEquals(results.get(2), new Tuple("2", "D", "F"));
    Assert.assertEquals(results.get(3), new Tuple("2", "H", "F"));

    Assert.assertEquals(4, results.size());
  }

}
