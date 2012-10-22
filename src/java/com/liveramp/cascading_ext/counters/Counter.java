package com.liveramp.cascading_ext.counters;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

public class Counter {
  private final String group;
  private final String name;
  private final Long value;

  public Counter(String group, String name, Long value) {
    this.group = group;
    this.name = name;
    this.value = value;
  }

  public String getGroup() {
    return group;
  }

  public String getName() {
    return name;
  }

  public Long getValue() {
    return value;
  }

  @Override
  public String toString() {
    return padSpaces(name, 19) + ": "
            + padSpaces(prettyValue(), 11)
            + " (" + group + ")";
  }

  @Override
  public boolean equals(Object other) {
    return other != null
            && other instanceof Counter
            && ((Counter) other).group.equals(group)
            && ((Counter) other).name.equals(name)
            && ((Counter) other).value.equals(value);
  }

  private String prettyValue() {
    if (value == null) return "null";
    if (name.contains("BYTES")) return FileUtils.byteCountToDisplaySize(value);
    return value.toString();
  }

  private static String padSpaces(String str, int num) {
    int numSpaces = Math.max(0, num - str.length());
    return str + StringUtils.repeat(" ", numSpaces);
  }
}