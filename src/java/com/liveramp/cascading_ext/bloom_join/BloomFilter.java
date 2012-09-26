package com.liveramp.cascading_ext.bloom_join;

import com.liveramp.cascading_ext.FixedSizeBitSet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
 * @see org.onelab.filter.Filter The general behavior of a filter
 *
 * @see <a href="http://portal.acm.org/citation.cfm?id=362692&dl=ACM&coll=portal">Space/Time Trade-Offs in Hash Coding with Allowable Errors</a>
 */
public class BloomFilter extends Filter {

  public static double falsePositiveRate(int numHashes, long vectorSize, long numElements) {
    int k = numHashes;
    long m = vectorSize;
    long n = numElements;
    // http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
    // Math.pow(1 - Math.pow(1-1/(double)m, k*n), k);
    return Math.pow(1.0 - Math.exp((double) -k * n / m), k);
  }

  /** The bit vector. */
  private FixedSizeBitSet bits;
  private long numElems = 0;

  /** Default constructor - use with readFields */
  public BloomFilter() {
    super();
  }

  /**
   * Constructor
   *
   * @param vectorSize The vector size of <i>this</i> filter.
   * @param nbHash The number of hash function to consider.
   * @param hashType type of the hashing function (see {@link com.liveramp.cascading_ext.hash.Hash}).
   */
  public BloomFilter(long vectorSize, int nbHash, int hashType) {
    super(vectorSize, nbHash, hashType);
    bits = new FixedSizeBitSet(this.vectorSize);
  }

  public BloomFilter(long vectorSize, int nbHash, int hashType, byte[] arr) {
    super(vectorSize, nbHash, hashType);
    bits = new FixedSizeBitSet(this.vectorSize, arr);
  }

  @Override
  public void add(Key key) {
    long[] h = hash.hash(key);
    hash.clear();

    for (int i = 0; i < nbHash; i++ ) {
      bits.set(h[i]);
    }
    numElems++ ;
  }

  @Override
  public boolean membershipTest(Key key) {
    long[] h = hash.hash(key);
    hash.clear();
    for (int i = 0; i < nbHash; i++ ) {
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
    return BloomFilter.falsePositiveRate(nbHash, getVectorSize(), numElems);
  }

  public void acceptAll() {
    bits.fill();
  }

  // Writable interface
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(numElems);
    out.write(bits.getRaw());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    numElems = in.readLong();
    byte[] bytes = new byte[FixedSizeBitSet.getNumBytesToStore(vectorSize)];
    in.readFully(bytes);
    bits = new FixedSizeBitSet(this.vectorSize, bytes);
  }
}
