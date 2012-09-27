package com.liveramp.cascading_ext.bloom;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.Serializable;

public interface BloomFilterLoader extends Serializable {
  public abstract Object loadFilter(FileSystem fs, String filterFile) throws IOException;

  public abstract boolean mayContain(Object filter, Object potential);
}
