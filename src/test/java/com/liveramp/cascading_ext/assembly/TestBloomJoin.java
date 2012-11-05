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

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.tap.TapHelper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestBloomJoin extends BloomAssemblyTestCase {

  protected Hfs output;
  protected Hfs output2;
  protected Hfs output3;

  @Before
  public void setUp() throws Exception {

    output = new Hfs(new SequenceFile(new Fields("lhs-key", "lhs-key2", "lhs-value", "rhs-key", "rhs-key2", "rhs-value")),
        getTestRoot() + "/output1");

    output2 = new Hfs(new SequenceFile(new Fields("lhs-key-renamed", "lhs-value-renamed", "lhs-key", "rhs-value")),
        getTestRoot() + "/output2");

    output3 = new Hfs(new SequenceFile(new Fields("key", "key2", "v1", "key3", "key4", "v2")),
        getTestRoot() + "/output3");
  }

  @Test
  public void testSingle() throws IOException {

    Pipe lhs = new Pipe("lhs");
    Pipe rhs = new Pipe("rhs");

    Pipe joined = new BloomJoin(
        lhs, new Fields("key", "key2"),
        rhs, new Fields("key", "key2"),
        new Fields("lhs-key", "lhs-key2", "lhs-value", "rhs-key", "rhs-key2", "rhs-value"));

    Map<String, Tap> input = new HashMap<String, Tap>();
    input.put("lhs", this.lhsStore);
    input.put("rhs", this.rhsStore);

    CascadingUtil.get().getFlowConnector().connect("Test BloomJoin", input, output, joined).complete();

    List<Tuple> tuples = TapHelper.getAllTuples(output);
    assertTrue(tuples.contains(new Tuple(bytes("1"), bytes("11"), "w-lhs", bytes("1"), bytes("11"), "a-rhs")));
    assertTrue(tuples.contains(new Tuple(bytes("1"), bytes("11"), "w-lhs", bytes("1"), bytes("11"), "b-rhs")));
    assertTrue(tuples.contains(new Tuple(bytes("2"), bytes("12"), "x-lhs", bytes("2"), bytes("12"), "c-rhs")));

    assertEquals(3, tuples.size());
  }

  @Test
  public void testIt() throws IOException {

    Pipe lhs = new Pipe("lhs");
    Pipe lhs2 = new Pipe("lhs2");
    Pipe rhs = new Pipe("rhs");
    Pipe rhs2 = new Pipe("rhs2");

    Pipe joined = new BloomJoin(lhs, new Fields("key", "key2"),
        rhs, new Fields("key", "key2"),
        new Fields("lhs-key", "lhs-key2", "lhs-value", "rhs-key", "rhs-key2", "rhs-value"));

    Pipe joined4 = new BloomJoin(lhs2, new Fields("key", "key2"),
        rhs, new Fields("key", "key2"),
        new Fields("key", "key2", "v1", "key3", "key4", "v2"));
    joined4 = new Pipe("joined4", joined4);

    Pipe joinedSplit = new Pipe("joined-split", joined);
    joined = new Each(joined, new Fields("lhs-key", "lhs-value"), new Identity(new Fields("lhs-key-renamed", "lhs-value-renamed")));

    Pipe joined2 = new BloomJoin(lhs, new Fields("key", "key2"),
        rhs2, new Fields("key", "key2"),
        new Fields("lhs-key", "lhs-key2", "lhs-value", "rhs-key", "rhs-key2", "rhs-value"));

    joined2 = new Each(joined2, new Fields("lhs-key", "rhs-value"), new Identity());

    Pipe joined3 = new BloomJoin(joined, new Fields("lhs-key-renamed"),
        joined2, new Fields("lhs-key"), BloomAssembly.CoGroupOrder.LARGE_RHS);

    Pipe output2 = new Pipe("joined3", joined3);

    Map<String, Tap> input = new HashMap<String, Tap>();
    input.put("lhs", this.lhsStore);
    input.put("lhs2", this.lhs2Store);
    input.put("rhs", this.rhsStore);
    input.put("rhs2", this.rhs2Store);

    Map<String, Tap> output = new HashMap<String, Tap>();
    output.put("joined-split", this.output);
    output.put("joined3", this.output2);
    output.put("joined4", this.output3);

    CascadingUtil.get().getFlowConnector().connect(input, output, joinedSplit, output2, joined4).complete();

    List<Tuple> tuples = TapHelper.getAllTuples(this.output);
    assertTrue(tuples.contains(new Tuple(bytes("1"), bytes("11"), "w-lhs", bytes("1"), bytes("11"), "a-rhs")));
    assertTrue(tuples.contains(new Tuple(bytes("1"), bytes("11"), "w-lhs", bytes("1"), bytes("11"), "b-rhs")));
    assertTrue(tuples.contains(new Tuple(bytes("2"), bytes("12"), "x-lhs", bytes("2"), bytes("12"), "c-rhs")));

    assertEquals(3, tuples.size());

    List<Tuple> tuples2 = TapHelper.getAllTuples(this.output2);
    assertTrue(tuples2.contains(new Tuple(bytes("1"), "w-lhs", bytes("1"), "a2-rhs")));
    assertTrue(tuples2.contains(new Tuple(bytes("1"), "w-lhs", bytes("1"), "b2-rhs")));
    assertTrue(tuples2.contains(new Tuple(bytes("1"), "w-lhs", bytes("1"), "a2-rhs")));
    assertTrue(tuples2.contains(new Tuple(bytes("1"), "w-lhs", bytes("1"), "b2-rhs")));
    assertTrue(tuples2.contains(new Tuple(bytes("2"), "x-lhs", bytes("2"), "c2-rhs")));

    assertEquals(5, tuples2.size());

    List<Tuple> tuples3 = TapHelper.getAllTuples(this.output3);
    assertTrue(tuples3.contains(new Tuple(bytes("1"), bytes("11"), "w-lhs2", bytes("1"), bytes("11"), "a-rhs")));
    assertTrue(tuples3.contains(new Tuple(bytes("1"), bytes("11"), "w-lhs2", bytes("1"), bytes("11"), "b-rhs")));
    assertTrue(tuples3.contains(new Tuple(bytes("2"), bytes("12"), "x-lhs2", bytes("2"), bytes("12"), "c-rhs")));

    assertEquals(3, tuples3.size());
  }
}
