package com.liveramp.cascading_ext.multi_group_by;

import cascading.tuple.Tuple;
import com.liveramp.cascading_ext.BaseTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * @author eddie
 */
public class TestSingleElementTupleIterator extends BaseTestCase {
  public void testIt() {
    List<Tuple> tuples = new ArrayList<Tuple>();
    tuples.add(new Tuple("tuple", "number", "one"));
    tuples.add(new Tuple("tuple", "number", "two"));
    tuples.add(new Tuple("tuple", "number", "three"));

    SingleElementTupleIterator<String> it = new SingleElementTupleIterator<String>(tuples.iterator(), 2);
    assertTrue(it.hasNext());
    assertEquals("one", it.next());
    assertTrue(it.hasNext());
    assertEquals("two", it.next());
    assertTrue(it.hasNext());
    assertEquals("three", it.next());
    assertFalse(it.hasNext());
  }
}
