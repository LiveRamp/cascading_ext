package com.liveramp.cascading_ext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
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

  public static final List<String> INPUT = Lists.newArrayList(
      "012345",
      "abcdef",
      "FIRsTH");

  public static final List<Tuple> EXPECTED = Lists.newArrayList(
      new Tuple("012", "3", "45"),
      new Tuple("abc", "d", "ef"),
      new Tuple("FIR", "s", "TH"));

  private final String testPath;

  public TestFixedWidthTextScheme() throws IOException {
    testPath = getTestRoot() + "/test-fixed-width-text-scheme.txt";
  }

  private void writeLine(FileOutputStream writer, String s) throws IOException {
    writer.write(s.getBytes(CHARSET));
    writer.write("\n".getBytes(CHARSET));
  }

  @Before
  public void setUp() throws Exception {
    fs.mkdirs(new Path(getTestRoot()));

    FileOutputStream writer = new FileOutputStream(new File(testPath));
    for (String line : INPUT) {
      writeLine(writer, line);
    }
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
    assertEquals(EXPECTED, tuplesRead);
  }

  @SuppressWarnings("unused")
  @Test
  public void testShortRead() throws IOException {
    FileOutputStream writer = new FileOutputStream(testPath, true);
    writeLine(writer, "short");
    writer.close();

    Tap in = new Hfs(new FixedWidthTextScheme(FIELDS, COLUMN_WIDTHS, CHARSET), testPath);
    try {
      List<Tuple> tuplesRead = getAllTuples(in);
      fail("Should have failed on short tuple");
    } catch (TupleException e) {
      assertTrue(e.getCause().getClass() == TapException.class);
    }

  }

  @SuppressWarnings("unused")
  @Test
  public void testLengthyRead() throws IOException {
    FileOutputStream writer = new FileOutputStream(testPath, true);
    writeLine(writer, "Lengthy");
    writer.close();

    Tap in = new Hfs(new FixedWidthTextScheme(FIELDS, COLUMN_WIDTHS, CHARSET), testPath);
    try {
      List<Tuple> tuplesRead = getAllTuples(in);
      fail("Should have failed on lengthy tuple");
    } catch (TupleException e) {
      assertTrue(e.getCause().getClass() == TapException.class);
    }

  }

  @SuppressWarnings("unused")
  @Test
  public void testColsFieldsMismatch() throws IOException {
   try {
     FixedWidthTextScheme scheme = new FixedWidthTextScheme(FIELDS.append(new Fields("toomuch")), COLUMN_WIDTHS, CHARSET);
     fail("Should have failed on colwidths-fields mismatch.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testNoFailureOnNonStrict() throws IOException {
    FileOutputStream writer = new FileOutputStream(testPath, true);
    writeLine(writer, "Lengthy");
    writeLine(writer, "Normal");
    writeLine(writer, "Short");
    writeLine(writer, "Normal");
    writeLine(writer, "Tiny");
    writer.close();

    List<Tuple> expected = new ArrayList<Tuple>();
    expected.addAll(EXPECTED);
    expected.add(new Tuple("Nor", "m", "al"));
    expected.add(new Tuple("Nor", "m", "al"));

    Tap in = new Hfs(new FixedWidthTextScheme(FIELDS, COLUMN_WIDTHS, CHARSET, false), testPath);
    List<Tuple> tuplesRead = getAllTuples(in);

    assertEquals(expected, tuplesRead);
  }

}
