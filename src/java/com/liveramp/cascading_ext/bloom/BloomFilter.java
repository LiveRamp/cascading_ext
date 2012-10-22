package com.liveramp.cascading_ext.bloom;

import com.liveramp.cascading_ext.FixedSizeBitSet;
import com.liveramp.cascading_ext.hash2.HashFunction;
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

  private FixedSizeBitSet bits;

  private HashFunction hashFunction;

  protected int numHashes;
  private long vectorSize;
  private long numElems;

  public BloomFilter() {}

  public BloomFilter(long vectorSize, int numHashes) {
    this(vectorSize, numHashes, new FixedSizeBitSet(vectorSize), 0);
  }

  public BloomFilter(long vectorSize, int numHashes, byte[] arr, long numElems) {
    this(vectorSize, numHashes, new FixedSizeBitSet(vectorSize, arr), numElems);
  }

  public BloomFilter(long vectorSize, int numHashes, FixedSizeBitSet bits, long numElems){
    this.vectorSize = vectorSize;
    this.numHashes = numHashes;
    this.bits = bits;
    this.hashFunction = BloomConstants.DEFAULT_HASH_FACTORY.getFunction(vectorSize, numHashes);
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
  public boolean membershipTest(byte[] key) {
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
    out.writeBytes(this.hashFunction.getHashID()+"\n");
    out.write(this.bits.getRaw());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    numHashes = in.readInt();
    vectorSize = in.readLong();
    numElems = in.readLong();
    String serilizedHashID = in.readLine();
    hashFunction = BloomConstants.DEFAULT_HASH_FACTORY.getFunction(vectorSize, numHashes);
    if(!serilizedHashID.equals(hashFunction.getHashID())){
      throw new RuntimeException("bloom filter was written with hash type "+serilizedHashID+
          " but current hash function type is "+hashFunction.getHashID()+"!");
    }

    byte[] bytes = new byte[FixedSizeBitSet.getNumBytesToStore(vectorSize)];
    in.readFully(bytes);
    bits = new FixedSizeBitSet(vectorSize, bytes);
  }
}