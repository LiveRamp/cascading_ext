package com.liveramp.cascading_ext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.junit.After;
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
  public static final Charset CHARSET = Charset.forName("UTF-8");

  private final String testPath;

  public TestFixedWidthTextScheme() {
    testPath = "/tmp/test-fixed-width-text-scheme.txt";
  }

  private void writeLine(FileOutputStream writer, String s) throws IOException {
    writer.write(s.getBytes(CHARSET));
    writer.write("\n".getBytes(CHARSET));
  }

  @Before
  public void setUp() throws Exception {
    FileOutputStream writer = new FileOutputStream(new File(testPath));
    writeLine(writer, "012345");
    writeLine(writer, "abcdef");
    writeLine(writer, "FIRsTH");
    writer.close();
  }

  @After
  public void tearDown() throws IOException {
    fs.delete(new Path(testPath), false);
  }

  @Test
  public void testRead() throws IOException {
    Tap in = new Hfs(new FixedWidthTextScheme(FIELDS, COLUMN_WIDTHS, CHARSET), testPath);
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

    Tap in = new Hfs(new FixedWidthTextScheme(FIELDS, COLUMN_WIDTHS, CHARSET), testPath);
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

    Tap in = new Hfs(new FixedWidthTextScheme(FIELDS, COLUMN_WIDTHS, CHARSET), testPath);
    try {
      List<Tuple> tuplesRead = getAllTuples(in);
      fail("Should have failed on lengthy tuple");
    } catch (TupleException e) {
      assertTrue(e.getCause().getClass() == TapException.class);
    }

  }

}