package com.liveramp.cascading_ext.fs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public interface DeleteAction {
  boolean delete(FileSystem fs, Path path) throws IOException;
}
