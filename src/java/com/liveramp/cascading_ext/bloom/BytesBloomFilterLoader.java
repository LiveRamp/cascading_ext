package com.liveramp.cascading_ext.bloom;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class BytesBloomFilterLoader implements BloomFilterLoader {

  @Override
  public Object loadFilter(FileSystem fs, String filterFile) throws IOException {
    return BytesBloomFilter.readFromFileSystem(fs, new Path(filterFile));
  }

  @Override
  public boolean mayContain(Object filter, Object potential) {
    BytesBloomFilter bbf = (BytesBloomFilter) filter;
    return bbf.mayContain((byte[]) potential);
  }
}
