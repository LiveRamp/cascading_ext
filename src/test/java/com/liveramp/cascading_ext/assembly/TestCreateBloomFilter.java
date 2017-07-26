package com.liveramp.cascading_ext.assembly;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Assert;
import org.junit.Test;

import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.bloom.*;
import com.liveramp.cascading_ext.tap.TapHelper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestCreateBloomFilter extends BaseTestCase {


  @Test
  public void testSingle() throws IOException {

    Hfs keyStore = new Hfs(new SequenceFile(new Fields("key")), getTestRoot() + "/keys");

    TapHelper.writeToTap(keyStore,
        new Tuple(bytes("1")),
        new Tuple(bytes("2")),
        new Tuple(bytes("red")),
        new Tuple(bytes("blue")));

    Pipe keyPipe = new Pipe("keys");

    com.liveramp.cascading_ext.bloom.BloomFilter filter =
        CreateBloomFilter.createBloomForKeys(keyStore, keyPipe, "key");

    Assert.assertTrue(filter.membershipTest("1".getBytes()));
    Assert.assertTrue(filter.membershipTest("2".getBytes()));
    Assert.assertTrue(filter.membershipTest("red".getBytes()));
    Assert.assertTrue(filter.membershipTest("blue".getBytes()));

    boolean atLeastOneIsNotInTheFilter = Lists.newArrayList("3", "4", "green", "yellow")
        .stream().map(String::getBytes)
        .anyMatch(key -> !filter.membershipTest(key));

    Assert.assertTrue(atLeastOneIsNotInTheFilter);
  }

  protected BytesWritable bytes(String key) {
    return new BytesWritable(key.getBytes());
  }

}
