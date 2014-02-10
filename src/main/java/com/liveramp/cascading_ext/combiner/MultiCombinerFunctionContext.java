package com.liveramp.cascading_ext.combiner;

import java.util.List;

public class MultiCombinerFunctionContext {

  public final List<CombinerFunctionContext> contexts;

  MultiCombinerFunctionContext(List<CombinerFunctionContext> contexts) {
    this.contexts = contexts;
  }
}
