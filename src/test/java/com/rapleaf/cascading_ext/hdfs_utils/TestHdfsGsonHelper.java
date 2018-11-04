package com.rapleaf.cascading_ext.hdfs_utils;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.generative.Generative;
import com.liveramp.java_support.functional.Fns8;

public class TestHdfsGsonHelper extends BaseTestCase {

  @Test
  public void testStreamObject() throws Exception {
    String json = "{'key1': 'ignore', 'key2':{'deepKey1': 1, 'deepKey2': 2}}".replaceAll("'", "\"");

    StringReader reader = new StringReader(json);

    Stream<Pair<String, Integer>> results = HdfsGsonHelper.streamObject(new Gson(), Integer.class, reader, "key2");

    List<Pair<String, Integer>> collect = results.collect(Collectors.toList());

    Assert.assertEquals(
        Lists.newArrayList(p("deepKey1", 1), p("deepKey2", 2)),
        collect
    );
  }

  @Test
  public void testReadWriteObjectGenerative() throws Exception {
    Generative.runTests(10, (testNum, gen) -> {

      List<String> path = gen.namedVar("path")
          .listOfLength(gen.anyStringOfLengthUpTo(1000), gen.anyBoundedInteger(0, 20)).get();

      HashMap<String, Integer> map = gen.anyStringOfLengthUpTo(1000).stream().map(s -> Pair.of(s, gen.anyPositiveInteger().get()))
          .limit(gen.anyBoundedInteger(0, 5000).get()).collect(Fns8.pairsToMap());

      Stream<Pair<String, Integer>> dataStream = map.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue()));

      FileSystem fs = FileSystemHelper.getFileSystemForPath(getTestRoot());
      Gson gson = new Gson();
      Path dataPath = new Path(getTestRootPath(), "data");
      HdfsGsonHelper.writeFromStream(fs, gson, dataPath, dataStream, Integer.class, path.toArray(new String[0]));

      HashMap<String, Integer> result =
          HdfsGsonHelper.streamObject(fs, gson, dataPath, Integer.class, path.toArray(new String[0])).collect(Fns8.pairsToMap());

      Assert.assertEquals(map, result);
    });
  }

  @NotNull
  private Pair<String, Integer> p(String deepKey1, int right) {
    return Pair.of(deepKey1, right);
  }

}