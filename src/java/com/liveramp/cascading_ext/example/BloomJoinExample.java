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

package com.liveramp.cascading_ext.example;

import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.assembly.BloomJoin;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * An example of how to use the BloomJoin class when using CascadingUtil.  In practice this isn't nearly enough data
 * to warrant using BloomJoin, but it will work fine for a test job.
 */
public class BloomJoinExample {

  public static void main(String[] args) throws IOException {
    if(args.length != 1){
      System.out.println("Usage: hadoop jar cascading_ext.job.jar com.liveramp.cascading_ext.example.BloomJoinExample <output dir>");
      return;
    }

    Map<String, Tap> sources = new HashMap<String, Tap>();
    sources.put("source1", ExampleFixtures.SOURCE_TAP_1);
    sources.put("source2", ExampleFixtures.SOURCE_TAP_2);

    String outputDir = args[0];
    Hfs sink = new Hfs(new SequenceFile(new Fields("field1", "field2", "field3", "field4")), outputDir);

    Pipe source1 = new Pipe("source1");
    Pipe source2 = new Pipe("source2");

    Pipe joined = new BloomJoin(source1, new Fields("field1"), source2, new Fields("field3"));

    CascadingUtil.get().getFlowConnector().connect("Example flow", sources, sink, joined).complete();

    //  Take a look at the output tuples
    TupleEntryIterator output = sink.openForRead(CascadingUtil.get().getFlowProcess());
    System.out.println("Output tuples from flow:");
    while(output.hasNext()){
      System.out.println(output.next().getTuple());
    }
  }
}
