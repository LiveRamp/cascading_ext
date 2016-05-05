package com.liveramp.cascading_ext;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestFileSystemHelper extends BaseTestCase {


  @Test
  public void testPrefixStripping() throws Exception {

    Path path = new Path(getTestRoot() + "/tmp");
    FSDataOutputStream file = fs.create(path);
    file.close();

    //  fine
    FileSystemHelper.safeDeleteOnExit(fs, path);

    //  so this makes sense both locally and on jenkins
    String rawPath = path.toUri().getPath();
    assertEquals('/', rawPath.charAt(0));  // w/e
    Path fakeViewFSPath = new Path("viewfs://" + rawPath);

    Path noPrefixPath = FileSystemHelper.stripPrefix(fakeViewFSPath);

    //  make sure these work
    fs.exists(noPrefixPath);
    fs.deleteOnExit(noPrefixPath);

    assertEquals(new Path(rawPath), noPrefixPath);

  }



}