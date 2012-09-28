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
  private transient HadoopGroupByClosure _closure = null;
  private Fields _resultFields;
  private SpillableTupleList _results;
  private int _pipeFieldsSum;

  public MultiBuffer(Fields resultFields) {
    _resultFields = resultFields;
  }

  public Fields getResultFields() {
    return _resultFields;
  }

  public void setContext(int pipeFieldsSum, HadoopGroupByClosure closure) {
    _closure = closure;
    _results = new HadoopSpillableTupleList(100000, null, (JobConf) closure.getFlowProcess().getConfigCopy());
    _pipeFieldsSum = pipeFieldsSum;
  }

  public SpillableTupleList getResults() {
    return _results;
  }

  public abstract void operate();

  protected void emit(Tuple result) {
    Tuple ret = new Tuple(getGroup());
    ret.addAll(result);
    while (ret.size() < _pipeFieldsSum) {
      ret.add(0);
    }
    _results.add(ret);
  }

  protected Iterator<Tuple> getArgumentsIterator(int pos) {
    return _closure.getIterator(pos);
  }

  protected <T> Iterator<T> getValuesIterator(int pos) {
    return getValuesIterator(pos, 0);
  }

  protected <T> Iterator<T> getValuesIterator(int pos, int field) {
    return new TupleIteratorWrapper<T>(getArgumentsIterator(pos), field);
  }

  public Tuple getGroup() {
    return _closure.getGrouping();
  }

  public FlowProcess getFlowProcess() {
    return _closure.getFlowProcess();
  }
}
