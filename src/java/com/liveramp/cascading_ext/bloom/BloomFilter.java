package com.liveramp.cascading_ext.bloom;

import com.liveramp.cascading_ext.FixedSizeBitSet;
import com.liveramp.cascading_ext.hash2.HashFunction;
import com.liveramp.cascading_ext.hash2.HashFunctionFactory;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
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
  private long numElems = 0;

  /** Default constructor - use with readFields */
  public BloomFilter() {}

  /**
   * Constructor
   *
   * @param vectorSize The vector size of <i>this</i> filter.
   */
  public BloomFilter(long vectorSize, int numHashes, HashFunctionFactory hashFactory) {
    this.numHashes = numHashes;
    this.vectorSize = vectorSize;
    this.bits = new FixedSizeBitSet(this.vectorSize);
    this.hashFunction = hashFactory.getFunction(vectorSize, numHashes);
  }

  public BloomFilter(long vectorSize, int numHashes, HashFunctionFactory hashFactory, byte[] arr) {
    this.vectorSize = vectorSize;
    this.numHashes = numHashes;
    this.bits = new FixedSizeBitSet(this.vectorSize, arr);
    this.hashFunction = hashFactory.getFunction(vectorSize, numHashes);
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

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.numHashes);
    out.writeLong(this.vectorSize);
    out.writeLong(this.numElems);
    out.writeBytes(serializeFilter(this.hashFunction));
    System.out.println(serializeFilter(this.hashFunction).length());
    out.write(bits.getRaw());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.numHashes = in.readInt();
    this.vectorSize = in.readLong();
    this.numElems = in.readLong();
    this.hashFunction = deserializeFilter(in.readLine());
    byte[] bytes = new byte[FixedSizeBitSet.getNumBytesToStore(vectorSize)];
    in.readFully(bytes);
    bits = new FixedSizeBitSet(this.vectorSize, bytes);
  }

  private static String serializeFilter(HashFunction function){
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
      ObjectOutputStream out = new ObjectOutputStream(bos) ;
      out.writeObject(function);
      out.close();
      return new String(Hex.encodeHex(bos.toByteArray()))+"\n";
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static HashFunction deserializeFilter(String str){
    try {
      byte[] serialized = Hex.decodeHex(str.toCharArray());
      ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(serialized));
      HashFunction func = (HashFunction) in.readObject();
      in.close();
      return func;
    } catch (DecoderException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}