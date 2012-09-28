package com.liveramp.cascading_ext.multi_group_by;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopGroupByClosure;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.collect.SpillableTupleList;
import cascading.tuple.hadoop.collect.HadoopSpillableTupleList;
import org.apache.hadoop.mapred.JobConf;

import java.io.Serializable;
import java.util.Iterator;

public abstract class MultiBuffer implements Serializable {
  private transient HadoopGroupByClosure closure = null;
  private Fields resultFields;
  private SpillableTupleList results;
  private int pipeFieldsSum;

  public MultiBuffer(Fields resultFields) {
    this.resultFields = resultFields;
  }

  public Fields getResultFields() {
    return resultFields;
  }

  public void setContext(int pipeFieldsSum, HadoopGroupByClosure closure) {
    this.closure = closure;
    results = new HadoopSpillableTupleList(100000, null, (JobConf) closure.getFlowProcess().getConfigCopy());
    this.pipeFieldsSum = pipeFieldsSum;
  }

  public SpillableTupleList getResults() {
    return results;
  }

  public abstract void operate();

  protected void emit(Tuple result) {
    Tuple ret = new Tuple(getGroup());
    ret.addAll(result);
    while (ret.size() < pipeFieldsSum) {
      ret.add(0);
    }
    results.add(ret);
  }

  protected Iterator<Tuple> getArgumentsIterator(int pos) {
    return closure.getIterator(pos);
  }

  protected <T> Iterator<T> getValuesIterator(int pos) {
    return getValuesIterator(pos, 0);
  }

  protected <T> Iterator<T> getValuesIterator(int pos, int field) {
    return new TupleIteratorWrapper<T>(getArgumentsIterator(pos), field);
  }

  public Tuple getGroup() {
    return closure.getGrouping();
  }

  public FlowProcess getFlowProcess() {
    return closure.getFlowProcess();
  }
}
