package com.liveramp.cascading_ext.assembly;

import java.util.HashMap;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

public class RenameInPlace extends SubAssembly {

  public RenameInPlace(Pipe input, Fields inputFields, Fields toRename, Fields newName){
    super(input);

    Map<Comparable, Comparable> swap = new HashMap<Comparable, Comparable>();
    for(int i = 0; i < toRename.size(); i++){
      swap.put(toRename.get(i), newName.get(i));
    }

    Fields resultFields = new Fields();
    for(int i = 0; i < inputFields.size(); i++){
      if(swap.containsKey(inputFields.get(i))){
        resultFields = resultFields.append(new Fields(swap.get(inputFields.get(i))));
      }else{
        resultFields = resultFields.append(new Fields(inputFields.get(i)));
      }
    }

    input = new Each(input,
        inputFields,
        new RenameFunction(resultFields),
        Fields.RESULTS);

    setTails(input);
  }

  public static class RenameFunction extends BaseOperation implements Function {

    public RenameFunction(Fields newOutput){
      super(newOutput);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
      functionCall.getOutputCollector().add(functionCall.getArguments().getTuple());
    }
  }
}
