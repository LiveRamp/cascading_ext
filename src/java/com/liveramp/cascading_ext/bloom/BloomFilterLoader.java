package com.liveramp.cascading_ext.bloom;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.Serializable;

public abstract class BloomFilterLoader implements Serializable {
  protected abstract Object loadFilter(FileSystem fs, String filterFile) throws IOException;

  protected abstract boolean mayContain(Object filter, Object potential);
}
