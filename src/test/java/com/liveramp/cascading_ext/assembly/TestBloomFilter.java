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

package com.liveramp.cascading_ext.assembly;

import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.bloom.BloomProps;
import com.liveramp.cascading_ext.tap.TapHelper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

public class TestBloomFilter extends BloomAssemblyTestCase {

  protected Hfs output;

  @Before
  public void setUp() throws Exception {
    output = new Hfs(new SequenceFile(new Fields("key", "key2", "lhs-value")), getTestRoot() + "/output");
  }

  @Test
  public void testExact() throws IOException {

    Pipe lhs = new Pipe("lhs");
    Pipe rhs = new Pipe("rhs");

    Pipe joined = new BloomFilter(lhs, new Fields("key", "key2"), rhs, new Fields("key", "key2"), true);

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

  @Test
  public void testInexact() throws IOException {

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

  @Test
  public void testSwapped() throws IOException {

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
  @Test
  public void testNumHashes() throws IOException {

    Pipe lhs = new Pipe("lhs");
    Pipe rhs = new Pipe("rhs");

    Pipe joined = new BloomFilter(lhs, new Fields("key", "key2"), rhs, new Fields("key", "key2"), true);

    Map<String, Tap> input = new HashMap<String, Tap>();
    input.put("lhs", this.lhsStore);
    input.put("rhs", this.rhsStore);

    Map<Object, Object> props = new Properties();
    props.put(BloomProps.MIN_BLOOM_HASHES, 5);
    props.put(BloomProps.MAX_BLOOM_HASHES, 6);

    CascadingUtil.get().getFlowConnector(props).connect(input, output, joined).complete();

    List<Tuple> tuples = TapHelper.getAllTuples(output);

    assertTrue(tuples.contains(new Tuple(bytes("1"), bytes("11"), "w-lhs")));
    assertTrue(tuples.contains(new Tuple(bytes("2"), bytes("12"), "x-lhs")));

    assertEquals(2, tuples.size());
  }

  @Test
  public void testNumHashes2() throws IOException {

    Pipe lhs = new Pipe("lhs");
    Pipe rhs = new Pipe("rhs");

    Pipe joined = new BloomFilter(lhs, new Fields("key", "key2"), rhs, new Fields("key", "key2"), true);

    Map<String, Tap> input = new HashMap<String, Tap>();
    input.put("lhs", this.lhsStore);
    input.put("rhs", this.rhsStore);

    Map<Object, Object> props = new Properties();
    props.put(BloomProps.MIN_BLOOM_HASHES, 6);
    props.put(BloomProps.MAX_BLOOM_HASHES, 6);

    CascadingUtil.get().getFlowConnector(props).connect(input, output, joined).complete();

    List<Tuple> tuples = TapHelper.getAllTuples(output);

    assertTrue(tuples.contains(new Tuple(bytes("1"), bytes("11"), "w-lhs")));
    assertTrue(tuples.contains(new Tuple(bytes("2"), bytes("12"), "x-lhs")));

    assertEquals(2, tuples.size());
  }

}
