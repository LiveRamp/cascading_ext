package com.rapleaf.cascading_ext.datastore;


import cascading.tap.Tap;
import cascading.tuple.TupleEntryIterator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * Base interface for all datastores used in workflows.
 */
public interface DataStore {

  public String getName();

  public Tap getTap();

  public String getPath();

  public String getRelPath();

}
