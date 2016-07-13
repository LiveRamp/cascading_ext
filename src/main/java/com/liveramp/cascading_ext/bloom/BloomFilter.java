/**
 * Copyright 2012 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.liveramp.cascading_ext.bloom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import com.liveramp.cascading_ext.FixedSizeBitSet;
import com.liveramp.cascading_ext.hash.HashFunction;
import com.liveramp.cascading_ext.hash.HashFunctionFactory;

/**
 * This bloom filter implementation is based on the org.apache.hadoop.util.bloom.BloomFilter implementation, but was
 * modified to allow 64 bit hashes and larger bloom filters.
 */
public class BloomFilter implements Writable {

  private FixedSizeBitSet bits;

  private HashFunction hashFunction;

  protected int numHashes;
  private long vectorSize;
  private long numElems;

  public BloomFilter() {
  }

  public BloomFilter(long vectorSize, int numHashes) {
    this(vectorSize, numHashes, new FixedSizeBitSet(vectorSize), 0);
  }

  public BloomFilter(long vectorSize, int numHashes, FixedSizeBitSet bits, long numElems) {
    this(vectorSize, numHashes, bits, numElems, HashFunctionFactory.DEFAULT_HASH_FACTORY);
  }

  public BloomFilter(long vectorSize, int numHashes, FixedSizeBitSet bits, long numElems, HashFunctionFactory hashFactory) {
    this.vectorSize = vectorSize;
    this.numHashes = numHashes;
    this.bits = bits;
    this.hashFunction = hashFactory.getFunction(vectorSize, numHashes);
    this.numElems = numElems;
  }

  /**
   * Adds a list of keys to <i>this</i> filter.
   *
   * @param keys The keys
   */
  public void add(Iterable<byte[]> keys) {
    for (byte[] key : keys) {
      add(key);
    }
  }

  /**
   * Adds a key to <i>this</i> filter.
   *
   * @param key The key to add.
   */
  public void add(byte[] key) {
    long[] h = hashFunction.hash(key);

    for (int i = 0; i < numHashes; i++) {
      bits.set(h[i]);
    }
    numElems++;
  }

  /**
   * Determines whether a specified key belongs to <i>this</i> filter.
   *
   * @param key The key to test.
   * @return boolean True if the specified key belongs to <i>this</i> filter.
   * False otherwise.
   */
  public boolean membershipTest(byte[] key) {
    long[] h = hashFunction.hash(key);
    for (int i = 0; i < numHashes; i++) {
      if (!bits.get(h[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return size of the the bloomfilter
   */
  public long getVectorSize() {
    return this.vectorSize;
  }

  /**
   * @return the expected false positive rate
   */
  public double getFalsePositiveRate() {
    return BloomUtil.getFalsePositiveRate(numHashes, getVectorSize(), numElems);
  }

  /**
   * Absorb elements from <i>other</i> into <i>this</i>. Membership test returns true
   * if key belongs to either filter.
   *
   * @param other The filter to union-absorb into <i>this</i>.
   */
  public void absorbUnion(BloomFilter other) {
    absorbCheck(other);
    bits.or(other.bits);
  }

  /**
   * Absorb elements only in both <i>other</i> and <i>this</i>. Membership test returns true
   * if key belongs to both filter.
   *
   * @param other The filter to intersection-absorb into <i>this</i>.
   */
  public void absorbIntersection(BloomFilter other) {
    absorbCheck(other);
    bits.and(other.bits);
  }

  private void absorbCheck(final BloomFilter other) {
    if (!hashFunction.getHashID().equals(other.hashFunction.getHashID())) {
      throw new IllegalArgumentException("Incompatible hash functions " + hashFunction.getHashID() + " and " + other.hashFunction.getHashID());
    }
    if (numHashes != other.numHashes) {
      throw new IllegalArgumentException("Incompatible number of hashes " + numHashes + " and " + other.numHashes);
    }
    if (vectorSize != other.vectorSize) {
      throw new IllegalArgumentException("Incompatible vector size " + vectorSize + " and " + other.vectorSize);
    }
  }

  public static BloomFilter read(FileSystem fs, Path path) throws IOException {
    FSDataInputStream inputStream = fs.open(path);
    BloomFilter bf = new BloomFilter();
    bf.readFields(inputStream);
    inputStream.close();
    return bf;
  }

  public void writeOut(FileSystem fs, Path path) throws IOException {
    FSDataOutputStream out = fs.create(path);
    write(out);
    out.close();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.numHashes);
    out.writeLong(this.vectorSize);
    out.writeLong(this.numElems);
    out.write(this.bits.getRaw());
    byte[] serializedHashFunction = SerializationUtils.serialize(this.hashFunction);
    out.writeInt(serializedHashFunction.length);
    out.write(serializedHashFunction);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    numHashes = in.readInt();
    vectorSize = in.readLong();
    numElems = in.readLong();
    byte[] bytes = new byte[FixedSizeBitSet.getNumBytesToStore(vectorSize)];
    in.readFully(bytes);
    bits = new FixedSizeBitSet(vectorSize, bytes);
    byte[] serializedHashFunction = new byte[in.readInt()];
    in.readFully(serializedHashFunction);
    hashFunction = (HashFunction)SerializationUtils.deserialize(serializedHashFunction);
  }
}
