package com.liveramp.cascading_ext;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestFixedWidthTextScheme extends BaseTestCase {

  private static final Fields FIELDS = new Fields("first", "second", "third");
  private static final List<Integer> COLUMN_WIDTHS = Lists.newArrayList(3, 1, 2);

  private final String testPath;

  public TestFixedWidthTextScheme() {
    testPath = "/tmp/test-fixed-width-text-scheme.txt";
  }

  @Before
  public void setUp() throws Exception {
    FileWriter writer = new FileWriter(new File(testPath));
    writer.write("012345\n");
    writer.write("abcdef\n");
    writer.write("FIRsTH\n");
    writer.close();

  }

  @Test
  public void testRead() throws IOException {
    Tap in = new Hfs(new FixedWidthTextScheme(FIELDS, COLUMN_WIDTHS), testPath);
    List<Tuple> tuplesRead = getAllTuples(in);
    assertEquals(
        Lists.newArrayList(
            new Tuple("012", "3", "45"),
            new Tuple("abc", "d", "ef"),
            new Tuple("FIR", "s", "TH")),
        tuplesRead);
  }

  @Test
  public void testShortRead() throws IOException {
    FileWriter writer = new FileWriter(new File(testPath));
    writer.append("short\n");
    writer.close();

    Tap in = new Hfs(new FixedWidthTextScheme(FIELDS, COLUMN_WIDTHS), testPath);
    try {
      List<Tuple> tuplesRead = getAllTuples(in);
      fail("Should have failed on short tuple");
    } catch (TupleException e) {
      assertTrue(e.getCause().getClass() == TapException.class);
    }

  }

  @Test
  public void testLengthyRead() throws IOException {
    FileWriter writer = new FileWriter(new File(testPath));
    writer.append("Lengthy\n");
    writer.close();

    Tap in = new Hfs(new FixedWidthTextScheme(FIELDS, COLUMN_WIDTHS), testPath);
    try {
      List<Tuple> tuplesRead = getAllTuples(in);
      fail("Should have failed on lengthy tuple");
    } catch (TupleException e) {
      assertTrue(e.getCause().getClass() == TapException.class);
    }

  }

}