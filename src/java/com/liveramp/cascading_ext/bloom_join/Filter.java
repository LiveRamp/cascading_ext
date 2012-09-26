package com.liveramp.cascading_ext.bloom_join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Defines the general behavior of a filter.
 * <p>
 * A filter is a data structure which aims at offering a lossy summary of a set <code>A</code>. The key idea is to map entries of <code>A</code> (also called
 * <i>keys</i>) into several positions in a vector through the use of several hash functions.
 * <p>
 * Typically, a filter will be implemented as a Bloom filter (or a Bloom filter extension).
 * <p>
 * It must be extended in order to define the real behavior.
 *
 * @see Filter The general behavior of a filter
 *
 * @version 1.0 - 2 Feb. 07
 *
 * @see Key The general behavior of a key
 * @see HashFunction A hash function
 */
public abstract class Filter implements Writable {
  private static final int VERSION = -1; // negative to accommodate for old format
  /** The vector size of <i>this</i> filter. */
  protected long vectorSize;

  /** The hash function used to map a key to several positions in the vector. */
  protected Hash64Function hash;

  /** The number of hash function to consider. */
  protected int nbHash;

  /** Type of hashing function to use. */
  protected int hashType;

  protected Filter() {}

  /**
   * Constructor.
   *
   * @param vectorSize The vector size of <i>this</i> filter.
   * @param nbHash The number of hash functions to consider.
   * @param hashType type of the hashing function (see {@link Hash}).
   */
  protected Filter(long vectorSize, int nbHash, int hashType) {
    this.vectorSize = vectorSize;
    this.nbHash = nbHash;
    this.hashType = hashType;
    this.hash = new Hash64Function(this.vectorSize, this.nbHash, this.hashType);
  }

  /**
   * Adds a key to <i>this</i> filter.
   *
   * @param key The key to add.
   */
  public abstract void add(Key key);

  /**
   * Determines whether a specified key belongs to <i>this</i> filter.
   *
   * @param key The key to test.
   * @return boolean True if the specified key belongs to <i>this</i> filter.
   *         False otherwise.
   */
  public abstract boolean membershipTest(Key key);

  /**
   * Adds a list of keys to <i>this</i> filter.
   *
   * @param keys The keys
   */
  public void add(Iterable<Key> keys) {
    for (Key key : keys) {
      add(key);
    }
  }

  /**
   * Adds an array of keys to <i>this</i> filter.
   *
   * @param keys The array of keys.
   */
  public void add(Key[] keys) {
    for (Key key : keys) {
      add(key);
    }
  }

  // Writable interface
  public void write(DataOutput out) throws IOException {
    out.writeInt(VERSION);
    out.writeInt(this.nbHash);
    out.writeByte(this.hashType);
    out.writeLong(this.vectorSize);
  }

  public void readFields(DataInput in) throws IOException {
    int ver = in.readInt();
    if (ver > 0) { // old unversioned format
      this.nbHash = ver;
      this.hashType = Hash.JENKINS_HASH;
    } else if (ver == VERSION) {
      this.nbHash = in.readInt();
      this.hashType = in.readByte();
    } else {
      throw new IOException("Unsupported version: " + ver);
    }
    this.vectorSize = in.readLong();
    this.hash = new Hash64Function(this.vectorSize, this.nbHash, this.hashType);
  }
}
