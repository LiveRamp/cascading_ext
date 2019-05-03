package com.liveramp.cascading_ext.fs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class DeleteUsingTrashIfEnabledAction implements DeleteAction {
  @Override
  public boolean delete(FileSystem fs, Path path) throws IOException {
    return TrashHelper.deleteUsingTrashIfEnabled(fs, path);
  }
}
