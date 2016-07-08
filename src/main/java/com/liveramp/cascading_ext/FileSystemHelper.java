/**
 * Copyright 2012 LiveRamp
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.liveramp.cascading_ext;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;

/**
 * This class contains helper methods for working with files, filepaths, and
 * FileSystems.
 */
public class FileSystemHelper {
  private static final int DEFAULT_FS_OP_NUM_TRIES = 3;
  private static final long DEFAULT_FS_OP_DELAY_BETWEEN_TRIES = 5 * 1000L;

  /**
   * @deprecated Please use {@link #getFileSystemForPath(String)} where possible.
   */
  @Deprecated
  public static FileSystem getFS() {
    try {
      return FileSystem.get(new Configuration());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static FileSystem getFileSystemForPath(String path) {
    return getFileSystemForPath(new Path(path));
  }

  public static FileSystem getFileSystemForPath(Path path) {
    try {
      return path.getFileSystem(new Configuration());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void createLocalFile(String path, String content) {
    try {
      DataOutputStream dos = new DataOutputStream(new FileOutputStream(new File(path)));
      dos.writeUTF(content);
      dos.close();
    } catch (IOException ioe) {
      throw new RuntimeException("failed to create local file", ioe);
    }
  }

  public static void createFile(FileSystem fs, String path, String content) throws IOException {
    FSDataOutputStream os = fs.create(new Path(path));
    os.write(content.getBytes());
    os.close();
  }

  /**
   * merge all files in <code>sourceDir</code> into local <code>targetFile</code>, retrying a few times on failure
   */
  public static void copyMergeToLocal(String srcDir, String dstFile) throws IOException {
    copyMergeToLocal(srcDir, dstFile, DEFAULT_FS_OP_NUM_TRIES, DEFAULT_FS_OP_DELAY_BETWEEN_TRIES);
  }

  /**
   * merge all files in <code>sourceDir</code> into local <code>targetFile</code>, retrying on failure
   */
  public static void copyMergeToLocal(String srcDir, String dstFile, int numTries, long delayBetweenTries) throws IOException {
    Configuration conf = new Configuration();
    FileSystem hdfs = getFS();
    FileSystem localfs = FileSystem.getLocal(conf);

    while (numTries-- > 0) {
      if (FileUtil.copyMerge(hdfs, new Path(srcDir), localfs, new Path(dstFile), false, conf, null)) {
        return;
      }
      try {
        Thread.sleep(delayBetweenTries);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    throw new IOException("Could not copyMerge from \"" + srcDir + "\" to \"" + dstFile + "\"!");
  }

  /**
   * Safely renames a path by retrying the operation <code>numTries</code> times
   * and sleeping <code>delayBetweenTries</code> seconds between each try. If it
   * still fails, it throws an IOException.
   *
   * @param fs                the filesystem object
   * @param src               the directory to be renamed
   * @param dst               the new name of the directory
   * @param numTries          number of tries to attempt the operation
   * @param delayBetweenTries the sleep delta between tries in millis
   * @throws IOException
   */
  public static void safeRename(FileSystem fs, Path src, Path dst, int numTries, long delayBetweenTries) throws IOException {
    while (numTries-- > 0) {
      if (fs.rename(src, dst)) {
        return;
      } else {
        try {
          Thread.sleep(delayBetweenTries);
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        }
      }
    }

    throw new IOException("Could not rename the file from \"" + src + "\" to \"" + dst + "\"!");
  }

  /**
   * Safely renames a path by retrying the operation 3 and sleeping 5000
   * milliseconds between tries. If it still fails, it throws an IOException.
   *
   * @param fs  the filesystem object
   * @param src the directory to be renamed
   * @param dst the new name of the directory
   * @throws IOException
   */
  public static void safeRename(FileSystem fs, Path src, Path dst) throws IOException {
    safeRename(fs, src, dst, DEFAULT_FS_OP_NUM_TRIES, DEFAULT_FS_OP_DELAY_BETWEEN_TRIES);
  }

  public static void safeRename(Path src, Path dst) throws IOException {
    safeRename(getFS(), src, dst, DEFAULT_FS_OP_NUM_TRIES, DEFAULT_FS_OP_DELAY_BETWEEN_TRIES);
  }


  /**
   * Safely mkdirs a directory by retrying the operation <code>numTries</code> times and sleeping <code>delayBetweenTries</code> milliseconds between each
   * try. If it still fails, it throws an IOException.
   *
   * @param fs                the filesystem object
   * @param dir               the directory to be created
   * @param numTries          number of tries to attempt the operation
   * @param delayBetweenTries the sleep delta between tries in millis
   * @throws IOException
   */
  public static void safeMkdirs(FileSystem fs, Path dir, int numTries, long delayBetweenTries) throws IOException {
    while (numTries-- > 0) {
      if (fs.mkdirs(dir)) {
        return;
      } else {
        try {
          Thread.sleep(delayBetweenTries);
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        }
      }
    }

    throw new IOException("Could not mkdirs the directory \"" + dir + "\"!");
  }

  /**
   * Safely mkdirs a directory by retrying the operation 3 times and sleeping
   * 5000 milliseconds between tries. If it still fails, it throws an
   * IOException.
   *
   * @param fs  the filesystem object
   * @param dir the directory to be created
   * @throws IOException
   */
  public static void safeMkdirs(FileSystem fs, Path dir) throws IOException {
    safeMkdirs(fs, dir, DEFAULT_FS_OP_NUM_TRIES, DEFAULT_FS_OP_DELAY_BETWEEN_TRIES);
  }

  /**
   * Creates a random path under a given path prefix
   *
   * @param pathPrefix
   * @return the random path
   */
  public static Path getRandomPath(Path pathPrefix) {
    return new Path(pathPrefix, UUID.randomUUID().toString());
  }

  /**
   * Creates a random path under a given path prefix
   *
   * @param pathPrefix
   * @return the random path
   */
  public static Path getRandomPath(String pathPrefix) {
    return getRandomPath(new Path(pathPrefix));
  }

  /**
   * Use this method to create a random temporary path that must be deleted upon
   * exit. It creates a random directory with a random file in it. The reason
   * for this is that hadoop requires the path to exist when it's marked for
   * deletion.
   *
   * @param pathPrefix the prefix under which the path is created
   * @return the random temporary path
   * @throws IOException
   */
  public static Path getRandomTemporaryPath(Path pathPrefix) throws IOException {
    Path randomTemporaryDir = getRandomPath(pathPrefix);
    FileSystem fs = getFS();
    fs.mkdirs(randomTemporaryDir);
    safeDeleteOnExit(fs, randomTemporaryDir);
    return getRandomPath(randomTemporaryDir);
  }


  protected static Path stripPrefix(Path path){
    String rawPath = path.toUri().getPath();
    return new Path(rawPath);
  }

  public static void safeDeleteOnExit(FileSystem fs, Path path) throws IOException {

    //  if it's a viewFS, get the child FS and attach the deleteOnExit to the right child
    //  (https://issues.apache.org/jira/browse/HDFS-10323)
    if(fs instanceof ViewFileSystem) {
      ViewFileSystem viewfs = (ViewFileSystem)fs;
      Path withoutPrefix = stripPrefix(path);

      for (FileSystem fileSystem : viewfs.getChildFileSystems()) {
        if (fileSystem.exists(withoutPrefix)) {
          fileSystem.deleteOnExit(withoutPrefix);
        }
      }
    }

    else{
      fs.deleteOnExit(path);
    }

  }

  /**
   * Gets a random temporary path that is deleted upon exit.
   *
   * @param pathPrefix the prefix under which the path is created
   * @return the random temporary path
   * @throws IOException
   */
  public static Path getRandomTemporaryPath(String pathPrefix) throws IOException {
    return getRandomTemporaryPath(new Path(pathPrefix));
  }

  /**
   * Gets a random temporary path that is deleted upon exit. The path will be
   * created under "/tmp"
   *
   * @return the random temporary path under "/tmp"
   * @throws IOException
   */
  public static Path getRandomTemporaryPath() throws IOException {
    return getRandomTemporaryPath("/tmp");
  }

  /**
   * Recursively print the path and children to stdout.
   */
  public static void printFiles(String path) throws IOException {
    FileSystem fs = getFS();
    if (fs.exists(new Path(path))) {
      printFiles(fs, new Path(path), 0);
    } else {
      System.out.println("no files at " + path);
    }
  }

  private static void printFiles(FileSystem fs, Path p, int indent) throws IOException {
    FileStatus stat = fs.getFileStatus(p);
    for (int i = 0; i < indent; i++) {
      System.out.print("\t");
    }
    System.out.println(p.toString());
    if (stat.isDir()) {
      for (FileStatus child : fs.listStatus(p)) {
        printFiles(fs, child.getPath(), indent + 1);
      }
    }
  }

  public static FileStatus[] safeListStatus(Path p) throws IOException {
    FileSystem fs = FileSystemHelper.getFileSystemForPath(p);
    return safeListStatus(fs, p);
  }

  public static FileStatus[] safeListStatus(Path p, PathFilter filter) throws IOException {
    FileSystem fs = FileSystemHelper.getFileSystemForPath(p);
    return safeListStatus(fs, p, filter);
  }

  public static FileStatus[] safeListStatus(FileSystem fs, Path p) throws IOException {
    return safeListStatus(fs, p, null);
  }

  public static FileStatus[] safeListStatus(FileSystem fs, Path p, PathFilter filter) throws IOException {
    try {
      if (filter == null) {
        return fs.listStatus(p);
      } else {
        return fs.listStatus(p, filter);
      }
    }
    //  CDH4 will throw FNFEs if p doesn't exist--let people safely check for files at a path
    catch (FileNotFoundException e) {
      return new FileStatus[0];
    }
  }
}
