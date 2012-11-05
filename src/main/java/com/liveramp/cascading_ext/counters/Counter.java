/**
 *  Copyright 2012 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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
