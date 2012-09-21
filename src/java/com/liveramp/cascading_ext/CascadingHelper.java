package com.liveramp.cascading_ext;

class CascadingHelper {
  private static final CascadingHelper instance = new CascadingHelper();

  private CascadingHelper(){}

  public static CascadingHelper get() {
    return instance;
  }
}