package com.liveramp.cascading_ext.assembly;

import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.bloom.BloomProps;
import com.liveramp.cascading_ext.tap.TapHelper;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;

public abstract class BloomAssemblyTestCase extends BaseTestCase {

  protected Tap lhsStore;
  protected Tap rhsStore;
  protected Tap lhs2Store;
  protected Tap rhs2Store;

  @Before
  public void bloomAssemblySetUp() throws Exception {
    CascadingUtil.get().setDefaultProperty(BloomProps.KEY_SAMPLE_RATE, 1.0);

    lhsStore = new Hfs(new SequenceFile(new Fields("key", "key2", "lhs-value")), getTestRoot() + "/lhs");
    lhs2Store = new Hfs(new SequenceFile(new Fields("key", "key2", "lhs-value")), getTestRoot() + "/lhs2");
    rhsStore = new Hfs(new SequenceFile(new Fields("key", "key2", "rhs-value")), getTestRoot() + "/rhs");
    rhs2Store = new Hfs(new SequenceFile(new Fields("key", "key2", "rhs-value")), getTestRoot() + "/rhs2");

    TapHelper.writeToTap(rhsStore,
        new Tuple(bytes("1"), bytes("11"), "a-rhs"),
        new Tuple(bytes("1"), bytes("11"), "b-rhs"),
        new Tuple(bytes("2"), bytes("12"), "c-rhs"),
        new Tuple(bytes("3"), bytes("13"), "d-rhs"));

    TapHelper.writeToTap(rhs2Store,
        new Tuple(bytes("1"), bytes("11"), "a2-rhs"),
        new Tuple(bytes("1"), bytes("11"), "b2-rhs"),
        new Tuple(bytes("2"), bytes("12"), "c2-rhs"),
        new Tuple(bytes("3"), bytes("13"), "d2-rhs"));

    TapHelper.writeToTap(lhsStore,
        new Tuple(bytes("1"), bytes("11"), "w-lhs"),
        new Tuple(bytes("2"), bytes("12"), "x-lhs"),
        new Tuple(bytes("4"), bytes("14"), "y-lhs"),
        new Tuple(bytes("5"), bytes("15"), "y-lhs"),
        new Tuple(bytes("6"), bytes("16"), "y-lhs"),
        new Tuple(bytes("7"), bytes("17"), "y-lhs"),
        new Tuple(bytes("8"), bytes("18"), "y-lhs"),
        new Tuple(bytes("9"), bytes("19"), "y-lhs"),
        new Tuple(bytes("10"), bytes("20"), "y-lhs"),
        new Tuple(bytes("11"), bytes("21"), "y-lhs"),
        new Tuple(bytes("12"), bytes("22"), "y-lhs"),
        new Tuple(bytes("13"), bytes("23"), "y-lhs"),
        new Tuple(bytes("14"), bytes("24"), "y-lhs"),
        new Tuple(bytes("15"), bytes("25"), "y-lhs"),
        new Tuple(bytes("16"), bytes("26"), "y-lhs"),
        new Tuple(bytes("17"), bytes("27"), "y-lhs"),
        new Tuple(bytes("18"), bytes("28"), "y-lhs"),
        new Tuple(bytes("19"), bytes("29"), "y-lhs"),
        new Tuple(bytes("20"), bytes("30"), "y-lhs"),
        new Tuple(bytes("21"), bytes("31"), "y-lhs")
    );

    TapHelper.writeToTap(lhs2Store,
        new Tuple(bytes("1"), bytes("11"), "w-lhs2"),
        new Tuple(bytes("2"), bytes("12"), "x-lhs2"),
        new Tuple(bytes("4"), bytes("14"), "y-lhs2"),
        new Tuple(bytes("5"), bytes("15"), "y-lhs2"),
        new Tuple(bytes("6"), bytes("16"), "y-lhs2"),
        new Tuple(bytes("7"), bytes("17"), "y-lhs2"),
        new Tuple(bytes("8"), bytes("18"), "y-lhs2"),
        new Tuple(bytes("9"), bytes("19"), "y-lhs2"),
        new Tuple(bytes("10"), bytes("20"), "y-lhs2"),
        new Tuple(bytes("11"), bytes("21"), "y-lhs2"),
        new Tuple(bytes("12"), bytes("22"), "y-lhs2"),
        new Tuple(bytes("13"), bytes("23"), "y-lhs2"),
        new Tuple(bytes("14"), bytes("24"), "y-lhs2"),
        new Tuple(bytes("15"), bytes("25"), "y-lhs2"),
        new Tuple(bytes("16"), bytes("26"), "y-lhs2"),
        new Tuple(bytes("17"), bytes("27"), "y-lhs2"),
        new Tuple(bytes("18"), bytes("28"), "y-lhs2"),
        new Tuple(bytes("19"), bytes("29"), "y-lhs2"),
        new Tuple(bytes("20"), bytes("30"), "y-lhs2"),
        new Tuple(bytes("21"), bytes("31"), "y-lhs2")
    );
  }

  protected BytesWritable bytes(String key) {
    return new BytesWritable(key.getBytes());
  }
}
