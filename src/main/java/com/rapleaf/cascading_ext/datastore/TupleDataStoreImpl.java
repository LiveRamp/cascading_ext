package com.rapleaf.cascading_ext.datastore;

import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import com.liveramp.cascading_tools.combine.BatchedSequenceFileScheme;

public class TupleDataStoreImpl extends DataStoreImpl implements TupleDataStore {
  
  protected final Fields fields;
  protected final Class hfsType; // Class of cascading schema (e.g. SequenceFile) stored in this store
  
  public TupleDataStoreImpl(String name, String persPath, String relPath, Fields fields, Class<? extends Scheme> hfsType) throws IOException {
    super(name, persPath, relPath);
    this.fields = fields;
    this.hfsType = hfsType;
  }
  
  public TupleDataStoreImpl(String name, String persPath, String relPath, Fields fields) throws IOException {
    super(name, persPath, relPath);
    this.fields = fields;
    this.hfsType = BatchedSequenceFileScheme.class;
  }
  
  public Hfs getTap() {
    return getTap(path);
  }
  
  public Hfs getTap(String path) {
    if (hfsType == BatchedSequenceFileScheme.class) {
      return new Hfs(new BatchedSequenceFileScheme(fields), path);
    } else if(hfsType == SequenceFile.class){
      return new Hfs(new SequenceFile(fields), path);
    } else if (hfsType == TextLine.class) {
      return new Hfs(new TextLine(fields), path);
    } else {
      throw new NotImplementedException("Support for schema " + hfsType + " has not been implemented in TupleDataStoreImp yet!");
    }
  }

  public long getDirectorySize(Configuration conf) throws IOException {
    FileSystem fs = new Path(path).getFileSystem(conf);
    return fs.getContentSummary(new Path(this.path)).getLength();
  }

  public Fields getFields() {
    return fields;
  }
}
