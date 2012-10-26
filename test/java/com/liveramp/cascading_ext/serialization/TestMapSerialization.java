package com.liveramp.cascading_ext.serialization;

import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.*;
import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class TestMapSerialization extends BaseTestCase {

  @Test
  public void testMapSerialization() throws IOException {
    // Build a map
    Map<String, String> map = new HashMap<String, String>();
    map.put("this", "is");
    map.put("a", "test");

    // Write the map to a tap
    Tap tap = new Hfs(new Fields("map"), getTestRoot() + "/map_test");
    TupleEntryCollector out = tap.openForWrite(CascadingUtil.get().getFlowProcess());
    out.add(new Tuple(map));
    out.close();

    // Make sure we read the same map back in
    TupleEntryIterator in = tap.openForRead(CascadingUtil.get().getFlowProcess());
    assertTrue(in.hasNext());
    TupleEntry tupleEntry = in.next();
    assertFalse(in.hasNext());
    assertEquals(map, tupleEntry.getObject(0));
  }
}
