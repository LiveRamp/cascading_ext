package com.liveramp.cascading_ext.fs;

import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.FileSystemHelper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TrashHelper {
  private static final Logger LOG = LoggerFactory.getLogger(TrashHelper.class);

  public static boolean moveToTrash(FileSystem fs, Path path) throws IOException {
    boolean move = Trash.moveToAppropriateTrash(fs, path, CascadingUtil.get().getJobConf());
    if(!move){
      throw new RuntimeException("Trash disabled or path already in trash: " + path);
    }

    return true;
  }

  public static boolean deleteUsingTrashIfEnabled(FileSystem fs, Path path) throws IOException {
    if(fs.exists(path)){
      if(isEnabled()){
        LOG.info("Moving to trash: " + path);
        return moveToTrash(fs, path);
      }else{
        LOG.info("Deleting: " + path);
        return fs.delete(path, true);
      }
    }

    //  if it wasn't there, consider it a success
    return true;
  }

  public static boolean isEnabled() throws IOException {

    //  check if enabled locally
    Integer interval = Integer.parseInt(CascadingUtil.get().getJobConf().get("fs.trash.interval"));

    if (interval != 0) {
      return true;
    }

    //  it could also be configured on the namenode rather than locally, so look there
    FileSystem fs = FileSystemHelper.getFS();
    long trashInterval = fs.getServerDefaults(new Path("/tmp/some_dummy_path")).getTrashInterval();

    return trashInterval != 0;
  }
}
