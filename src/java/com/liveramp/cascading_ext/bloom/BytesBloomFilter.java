package com.liveramp.cascading_ext.bloom;

import com.liveramp.cascading_ext.hash2.HashFunctionFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Deprecated
public class BytesBloomFilter implements Writable {
  private BloomFilter filter;

  public static BytesBloomFilter readFromFileSystem(FileSystem fs, Path p) throws IOException {
    BytesBloomFilter ret = new BytesBloomFilter();
    FSDataInputStream is = fs.open(p);
    ret.readFields(is);
    is.close();
    return ret;
  }

  public BytesBloomFilter() {
    filter = new BloomFilter();
  }

  public BytesBloomFilter(long vectorLength, int numHashes, HashFunctionFactory hashFunctionFactory) {
    filter = new BloomFilter(vectorLength, numHashes, hashFunctionFactory);
  }

  public BytesBloomFilter(long vectorLength, int numHashes, HashFunctionFactory hashFunctionFactory, byte[] arr) {
    filter = new BloomFilter(vectorLength, numHashes, hashFunctionFactory, arr);
  }

  public void add(byte[] bytes) {
    filter.add(new Key(bytes));
  }

  public boolean mayContain(byte[] bytes) {
    return filter.membershipTest(new Key(bytes));
  }

  public void readFields(DataInput in) throws IOException {
    filter.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    filter.write(out);
  }

  public void writeToFileSystem(FileSystem fs, Path p) throws IOException {
    FSDataOutputStream os = fs.create(p);
    write(os);
    os.close();
  }
}
