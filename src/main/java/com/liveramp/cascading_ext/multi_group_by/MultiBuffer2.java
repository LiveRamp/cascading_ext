package com.liveramp.cascading_ext.multi_group_by;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopGroupByClosure;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import java.io.Serializable;
import java.util.Iterator;

public abstract class MultiBuffer2 implements Serializable {
  private transient HadoopGroupByClosure closure = null;
  private Fields resultFields;

  private BufferCall call;
  private FlowProcess process;

  public MultiBuffer2(Fields resultFields) {
    this.resultFields = resultFields;
  }

  public Fields getResultFields() {
    return resultFields;
  }

  public void setContext(BufferCall bufferCall, FlowProcess flowProcess) {
    this.call = bufferCall;
    this.process = flowProcess;
    this.closure = (HadoopGroupByClosure) bufferCall.getJoinerClosure();
  }

  public abstract void operate();

  protected void emit(Tuple result) {
    call.getOutputCollector().add(result);
  }

  protected Iterator<Tuple> getArgumentsIterator(int pos) {
    return closure.getIterator(pos);
  }

  protected <T> Iterator<T> getValuesIterator(int pos) {
    return getValuesIterator(pos, 0);
  }

  protected <T> Iterator<T> getValuesIterator(int pos, int field) {
    return new SingleElementTupleIterator<T>(getArgumentsIterator(pos), field);
  }

  public Tuple getGroup() {
    return closure.getGrouping();
  }


  public FlowProcess getFlowProcess() {
    return process;
  }
}