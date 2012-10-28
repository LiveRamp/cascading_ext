package com.liveramp.cascading_ext.assembly;

import cascading.flow.FlowProcess;
import cascading.flow.planner.Scope;
import cascading.operation.*;
import cascading.pipe.*;
import cascading.pipe.assembly.Discard;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.Joiner;
import cascading.property.ConfigDef;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.liveramp.cascading_ext.FileSystemHelper;
import com.liveramp.cascading_ext.TupleSerializationUtil;
import com.liveramp.cascading_ext.bloom.BloomProps;
import com.liveramp.cascading_ext.bloom.operation.BloomJoinFilter;
import com.liveramp.cascading_ext.joiner.LimitJoin;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

/**
 * This SubAssembly is used by BloomJoin and BloomFilter. It builds a bloom filter from the RHS, filters
 * the LHS using the bloom filter and, depending on parameters, does a CoGroup for exactness.
 */
public abstract class BloomAssembly extends SubAssembly {
  private static Logger LOG = Logger.getLogger(BloomAssembly.class);

  protected enum Mode {
    FILTER_EXACT, FILTER_INEXACT, JOIN
  }

  public static enum CoGroupOrder {
    LARGE_LHS, LARGE_RHS
  }

  /**
   * See BloomJoin(...) for other argument usages.
   *
   * @param operationType  whether the operation is a join, filter, or inexact filter (passed in by subclasses)
   */
  protected BloomAssembly(Pipe largePipe, Fields largeJoinFields,
                          Pipe smallPipe, Fields smallJoinFields,
                          Fields renameFields, Mode operationType,
                          Joiner joiner, CoGroupOrder coGroupOrder) {

    try {

      String bloomJobID = UUID.randomUUID().toString();
      Path bloomTempDir = FileSystemHelper.getRandomTemporaryPath("/tmp/bloom_tmp/");

      String bloomPartsDir = bloomTempDir + "/parts";
      String bloomFinalFilter = bloomTempDir + "/filter.bloomfilter";
      String approxCountPartsDir = bloomTempDir + "/approx_distinct_keys_parts/";

      //  if it's a filter, we care about nothing except the join keys on the RHS -- remove the rest
      if (operationType != Mode.JOIN) {
        smallPipe = new Each(smallPipe, smallJoinFields, new Identity());
      }
      Pipe rhsOrig = new Pipe("smallPipe-orig", smallPipe);

      smallPipe = new Each(smallPipe, smallJoinFields, new GetSerializedTuple());
      smallPipe = new CreateBloomFilter(smallPipe, bloomJobID, approxCountPartsDir, bloomPartsDir, "serialized-tuple-key");

      // This is a bit of a hack to:
      //  1) Force a dependency on the operations performed on RHS above (can't continue until they're done)
      //  2) Bind RHS to the flow, which wouldn't happen otherwise.
      // Note that RHS has no output, so there shouldn't be any danger in doing this.
      Pipe filterPipe = new NaiveMerge(largePipe.getName(), largePipe, smallPipe);

      // Load the bloom filter into memory and apply it to the LHS.
      filterPipe = new Each(filterPipe, largeJoinFields, new BloomJoinFilter(bloomJobID, false));

      ConfigDef config = filterPipe.getStepConfigDef();  // tell BloomAssemblyStrategy which bloom filter to expect
      config.setProperty(BloomProps.SOURCE_BLOOM_FILTER_ID, bloomJobID);
      config.setProperty(BloomProps.REQUIRED_BLOOM_FILTER_PATH, bloomFinalFilter);

      if (operationType == Mode.FILTER_EXACT) {
        // We don't actually care about the fields on the RHS (the user just expects the LHS fields), so we can
        // rename them to junk to avoid conflicts with field names on the LHS
        Fields newKeyFields = new Fields();
        for (int i = 0; i < smallJoinFields.size(); i++) {
          newKeyFields = newKeyFields.append(new Fields("__bloom_join_tmp_" + i));
        }
        rhsOrig = new Each(rhsOrig, smallJoinFields, new Identity(newKeyFields), newKeyFields);
        filterPipe = getCoGroup(filterPipe, largeJoinFields, rhsOrig, newKeyFields, renameFields, joiner, coGroupOrder, operationType);

        filterPipe = new Discard(filterPipe, newKeyFields);
      } else if (operationType == Mode.JOIN) {
        filterPipe = getCoGroup(filterPipe, largeJoinFields, rhsOrig, smallJoinFields, renameFields, joiner, coGroupOrder, operationType);
      }

      setTails(filterPipe);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Pipe getCoGroup(Pipe filtered, Fields largeJoinFields,
                          Pipe keysOrig, Fields keyFields,
                          Fields renameFields,
                          Joiner joinerInput,
                          CoGroupOrder coGroupOrder,
                          Mode mode) {
    Pipe lhs, rhs;
    Fields lhsFields, rhsFields;
    Joiner joiner = new InnerJoin();

    if (coGroupOrder == CoGroupOrder.LARGE_LHS) {
      lhs = filtered;
      lhsFields = largeJoinFields;
      rhs = keysOrig;
      rhsFields = keyFields;
      if (mode == Mode.FILTER_EXACT) {
        // Limit join takes the LHS exactly once if and only if there is a match on the RHS.
        // Won't duplicate tuples on the LHS if there are duplicates on the RHS.
        joiner = new LimitJoin(new long[]{Long.MAX_VALUE, 1l});
      } else {
        if (joinerInput != null) {
          joiner = joinerInput;
        }
      }
    } else {
      lhs = keysOrig;
      lhsFields = keyFields;
      rhs = filtered;
      rhsFields = largeJoinFields;
      if (mode == Mode.FILTER_EXACT) {
        joiner = new LimitJoin(new long[]{1l, Long.MAX_VALUE});
      } else {
        if (joinerInput != null) {
          joiner = joinerInput;
        }
      }
    }

    if (renameFields != null) {
      return new CoGroup(lhs, lhsFields, rhs, rhsFields, renameFields, joiner);
    } else {
      return new CoGroup(lhs, lhsFields, rhs, rhsFields, joiner);
    }
  }

  private static class NaiveMerge extends Merge {
    String toAccept;

    public NaiveMerge(String toAccept, Pipe... pipes) {
      super(null, pipes);
      this.toAccept = toAccept;
    }

    @Override
    public Scope outgoingScopeFor(Set<Scope> incomingScopes) {
      Scope toUse = null;
      for (Scope s : incomingScopes) {
        if (s.getName().equals(toAccept)) {
          toUse = s;
        }
      }
      return new Scope(super.outgoingScopeFor(Collections.singleton(toUse)));
    }
  }

  private static class GetSerializedTuple extends BaseOperation implements Function {

    private transient TupleSerializationUtil tupleSerializationUtil;

    public GetSerializedTuple() {
      super(new Fields("serialized-tuple-key"));
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
      tupleSerializationUtil = new TupleSerializationUtil((JobConf) flowProcess.getConfigCopy());
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
      try {
        Tuple tuple = functionCall.getArguments().getTuple();
        byte[] tupleSerialized = tupleSerializationUtil.serialize(tuple);
        functionCall.getOutputCollector().add(new Tuple(new BytesWritable(tupleSerialized)));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
