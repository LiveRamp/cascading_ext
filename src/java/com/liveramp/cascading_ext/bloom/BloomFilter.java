package com.liveramp.cascading_ext.bloom;

import com.liveramp.cascading_ext.FixedSizeBitSet;
import com.liveramp.cascading_ext.hash2.HashFunction;
import com.liveramp.cascading_ext.hash2.HashFunctionFactory;
import com.liveramp.cascading_ext.hash2.HashTokenMap;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Writable;

import java.io.*;

/**
 * Implements a <i>Bloom filter</i>, as defined by Bloom in 1970.
 * <p>
 * The Bloom filter is a data structure that was introduced in 1970 and that has been adopted by the networking research community in the past decade thanks to
 * the bandwidth efficiencies that it offers for the transmission of set membership information between networked hosts. A sender encodes the information into a
 * bit vector, the Bloom filter, that is more compact than a conventional representation. Computation and space costs for construction are linear in the number
 * of elements. The receiver uses the filter to test whether various elements are members of the set. Though the filter will occasionally return a false
 * positive, it will never return a false negative. When creating the filter, the sender can choose its desired point in a trade-off between the false positive
 * rate and the size.
 *
 * contract <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
 *
 * @version 1.0 - 2 Feb. 07
 *
 * @see <a href="http://portal.acm.org/citation.cfm?id=362692&dl=ACM&coll=portal">Space/Time Trade-Offs in Hash Coding with Allowable Errors</a>
 */
public class BloomFilter implements Writable {

  private final HashTokenMap map;

  private FixedSizeBitSet bits;
  private HashFunction hashFunction;
  protected int numHashes;
  private long vectorSize;
  private long numElems;

  public BloomFilter(HashTokenMap map) {
    this.map = map;
  }

  public BloomFilter(long vectorSize, int numHashes, HashFunctionFactory hashFactory, HashTokenMap map) {
    this(vectorSize, numHashes, hashFactory.getFunction(vectorSize, numHashes), new FixedSizeBitSet(vectorSize), 0, map);
  }

  public BloomFilter(long vectorSize, int numHashes, HashFunction hashFunction, HashTokenMap map) {
    this(vectorSize, numHashes, hashFunction, new FixedSizeBitSet(vectorSize), 0, map);
  }

  public BloomFilter(long vectorSize, int numHashes, HashFunction hashFunction, byte[] arr, long numElems, HashTokenMap map) {
    this(vectorSize, numHashes, hashFunction, new FixedSizeBitSet(vectorSize, arr), numElems, map);
  }

  public BloomFilter(long vectorSize, int numHashes, HashFunction hashFunction, FixedSizeBitSet bits, long numElems, HashTokenMap map){
    this.vectorSize = vectorSize;
    this.numHashes = numHashes;
    this.bits = bits;
    this.hashFunction = hashFunction;
    this.numElems = numElems;
    this.map = map;
  }

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

  /**
   * Adds a key to <i>this</i> filter.
   *
   * @param key The key to add.
   */
  public void add(Key key) {
    long[] h = hashFunction.hash(key);

    for (int i = 0; i < numHashes; i++ ) {
      bits.set(h[i]);
    }
    numElems++ ;
  }

  /**
   * Determines whether a specified key belongs to <i>this</i> filter.
   *
   * @param key The key to test.
   * @return boolean True if the specified key belongs to <i>this</i> filter.
   *         False otherwise.
   */
  public boolean membershipTest(Key key) {
    long[] h = hashFunction.hash(key);
    for (int i = 0; i < numHashes; i++ ) {
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

  public static BloomFilter read(FileSystem fs, Path path, HashTokenMap map) throws IOException, InstantiationException, IllegalAccessException {
    FSDataInputStream inputStream = fs.open(path);
    BloomFilter bf = new BloomFilter(map);
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
    out.writeInt(map.getToken(this.hashFunction));
    out.write(this.bits.getRaw());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    numHashes = in.readInt();
    vectorSize = in.readLong();
    numElems = in.readLong();
    hashFunction = map.getFunction(in.readInt(), vectorSize, numHashes);
    byte[] bytes = new byte[FixedSizeBitSet.getNumBytesToStore(vectorSize)];
    in.readFully(bytes);
    bits = new FixedSizeBitSet(vectorSize, bytes);
  }
}