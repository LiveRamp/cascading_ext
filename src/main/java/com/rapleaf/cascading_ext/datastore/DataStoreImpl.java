package com.rapleaf.cascading_ext.datastore;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public abstract class DataStoreImpl implements DataStore {
  
  protected final String name;
  protected final String path;
  protected final String relPath;
  
  public DataStoreImpl(String name, String rootPath, String relPath) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(rootPath);
    Preconditions.checkNotNull(relPath);

    this.name = name;
    this.path = rootPath + relPath;
    this.relPath = relPath;
  }

  public String getName() {
    return name;
  }
  
  public String getPath() {
    return path;
  }
  
  public String getRelPath() {
    return relPath;
  }
}
