package com.liveramp.cascading_ext.assembly;

import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.tap.TapHelper;

public class TestBloomFilter extends BloomAssemblyTestCase {

  protected Tap output;

  public void setUp() throws Exception {
    super.setUp();

    output = new Hfs(new SequenceFile(new Fields("key", "key2", "lhs-value")),
        getTestRoot()+"/output");
  }

  public void testExact() throws IOException {
    BloomAssembly.DEFAULT_SAMPLE_RATE = 1.0;

    Pipe lhs = new Pipe("lhs");
    Pipe rhs = new Pipe("rhs");

    Pipe joined = new BloomFilter(lhs, new Fields("key", "key2"),
        rhs, new Fields("key", "key2"), true);

    Map<String, Tap> input = new HashMap<String, Tap>();
    input.put("lhs", this.lhsStore);
    input.put("rhs", this.rhsStore);

    CascadingUtil.get().getFlowConnector().connect(input, output, joined).complete();

    List<Tuple> tuples = TapHelper.getAllTuples(output);

    //    [32 31 32 x-lhs, 32 31 32 x-lhs]
    assertTrue(tuples.contains(new Tuple(bytes("1"), bytes("11"), "w-lhs")));
    assertTrue(tuples.contains(new Tuple(bytes("2"), bytes("12"), "x-lhs")));

    assertEquals(2, tuples.size());
  }

  public void testInexact() throws IOException {
    BloomAssembly.DEFAULT_SAMPLE_RATE = 1.0;

    Pipe lhs = new Pipe("lhs");
    Pipe rhs = new Pipe("rhs");

    Pipe joined = new BloomFilter(lhs, new Fields("key", "key2"),
        rhs, new Fields("key", "key2"), false);

    Map<String, Tap> input = new HashMap<String, Tap>();
    input.put("lhs", this.lhsStore);
    input.put("rhs", this.rhsStore);

    CascadingUtil.get().getFlowConnector().connect(input, output, joined).complete();

    List<Tuple> tuples = TapHelper.getAllTuples(output);

    assertTrue(tuples.contains(new Tuple(bytes("1"), bytes("11"), "w-lhs")));
    assertTrue(tuples.contains(new Tuple(bytes("2"), bytes("12"), "x-lhs")));

    assertTrue(tuples.size() > 2);
  }

  public void testSwapped() throws IOException {
    BloomAssembly.DEFAULT_SAMPLE_RATE = 1.0;

    Pipe lhs = new Pipe("lhs");
    Pipe rhs = new Pipe("rhs");

    Pipe joined = new BloomFilter(lhs, new Fields("key", "key2"),
        rhs, new Fields("key", "key2"), true, BloomAssembly.CoGroupOrder.LARGE_RHS);

    Map<String, Tap> input = new HashMap<String, Tap>();
    input.put("lhs", this.lhsStore);
    input.put("rhs", this.rhsStore);

    CascadingUtil.get().getFlowConnector().connect(input, output, joined).complete();

    List<Tuple> tuples = TapHelper.getAllTuples(output);

    assertTrue(tuples.contains(new Tuple(bytes("1"), bytes("11"), "w-lhs")));
    assertTrue(tuples.contains(new Tuple(bytes("2"), bytes("12"), "x-lhs")));

    assertTrue(tuples.size() == 2);
  }
}
