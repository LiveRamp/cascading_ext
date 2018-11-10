package com.rapleaf.cascading_ext.datastore;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public interface TupleDataStore extends DataStore {

  public Hfs getTap(String path);

  public Fields getFields();

  public long getDirectorySize(Configuration conf) throws IOException;

}
