package com.liveramp.cascading_ext.bloom_join;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class BytesBloomFilterLoader extends BloomFilterLoader {

  @Override
  protected Object loadFilter(FileSystem fs, String filterFile) throws IOException {
    return BytesBloomFilter.readFromFileSystem(fs, new Path(filterFile));
  }

  @Override
  protected boolean mayContain(Object filter, Object potential) {
    BytesBloomFilter bbf = (BytesBloomFilter) filter;
    return bbf.mayContain((byte[]) potential);
  }
}
