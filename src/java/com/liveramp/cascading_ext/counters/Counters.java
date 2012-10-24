package com.liveramp.cascading_ext.counters;

import cascading.flow.Flow;
import cascading.stats.CascadingStats;
import cascading.stats.FlowStats;
import cascading.stats.FlowStepStats;
import cascading.tap.Tap;
import org.apache.commons.lang.StringUtils;

import java.util.*;

public class Counters {
  /**
   * Use these helper methods to safely retrieve Hadoop counters.  Sometimes counters
   * get pushed off the job tracker too quickly so when we try to retrieve a missing
   * counter we get a NPE which kills the process.  At this point the counter is gone
   * so we might as well keep going.
   */
  public static Long safeGetWithDefault(FlowStats flowStats, String group, String name, Long defaultVal) {
    Long val = safeGet(flowStats, group, name);
    return val == null ? defaultVal : val;
  }

  public static Long safeGet(CascadingStats flowStats, String group, String name) {
    try {
      return get(flowStats, group, name);
    } catch (Exception e) {
      return null;
    }
  }

  @SuppressWarnings("rawtypes")
  public static Long safeGetWithDefault(CascadingStats flowStats, Enum counter, Long defaultVal) {
    Long val = safeGet(flowStats, counter);
    return val == null ? defaultVal : val;
  }

  @SuppressWarnings("rawtypes")
  public static Long safeGet(CascadingStats flowStats, Enum counter) {
    try {
      return get(flowStats, counter);
    } catch (Exception e) {
      return null;
    }
  }

  public static Long get(CascadingStats flowStats, String group, String name) {
    return flowStats.getCounterValue(group, name);
  }

  @SuppressWarnings("rawtypes")
  public static Long get(CascadingStats flowStats, Enum counter) {
    return flowStats.getCounterValue(counter);
  }

  /**
   * Get all counters for a given flow.  It returns a map keyed on the step stats object to
   * a list of all the counter objects for that step
   */
  public static Map<FlowStepStats, List<Counter>> getCountersByStep(Flow flow) {
    return getCountersByStep(flow.getFlowStats());
  }

  public static Map<FlowStepStats, List<Counter>> getCountersByStep(FlowStats flowStats) {
    Map<FlowStepStats, List<Counter>> counters = new HashMap<FlowStepStats, List<Counter>>();

    for (FlowStepStats statsForStep : flowStats.getFlowStepStats()) {
      if (!counters.containsKey(statsForStep)) {
        counters.put(statsForStep, new ArrayList<Counter>());
      }

      for (String group : safeGetCounterGroups(statsForStep)) {
        for (String name : statsForStep.getCountersFor(group)) {
          counters.get(statsForStep).add(new Counter(group, name, Counters.safeGet(statsForStep, group, name)));
        }
      }
    }

    return counters;
  }

  public static Collection<String> safeGetCounterGroups(FlowStepStats stats) {
    try {
      return stats.getCounterGroups();
    } catch (Exception e) {
      return Collections.emptyList();
    }
  }

  public static List<Counter> getCounters(Flow flow) {
    return getCounters(flow.getFlowStats());
  }

  public static List<Counter> getCounters(FlowStats flowStats) {
    List<Counter> counters = new ArrayList<Counter>();

    for (FlowStepStats step : flowStats.getFlowStepStats()) {
      for (String group : safeGetCounterGroups(step)) {
        for (String name : step.getCountersFor(group)) {
          counters.add(new Counter(group, name, Counters.safeGet(flowStats, group, name)));
        }
      }
    }

    return counters;
  }

  public static void printCounters(Flow flow) {
    System.out.println(prettyCountersString(flow));
  }

  public static String prettyCountersString(Flow flow) {
    Map<FlowStepStats, List<Counter>> counters = Counters.getCountersByStep(flow);
    StringBuilder builder = new StringBuilder("\n").append(StringUtils.repeat("=", 90)).append("\n");

    builder.append("Counters for ").append(flow.getName() == null ? "unnamed flow" : "flow " + flow.getName()).append("\n")
        .append("  with input ").append(prettyTaps(flow.getSources())).append("\n")
        .append("  and output ").append(prettyTaps(flow.getSinks())).append("\n");

    for (Map.Entry<FlowStepStats, List<Counter>> entry : counters.entrySet()) {
      builder.append("  Step: ").append(entry.getKey().getName()).append("\n");

      if (entry.getValue().isEmpty()) {
        builder.append("    No counters found.\n");
        continue;
      }

      boolean anyTuplesRead = false;
      boolean anyTuplesWritten = false;
      for (Counter counter : entry.getValue()) {
        if (counter.getValue() != null && counter.getValue() > 0) {
          builder.append("    ").append(counter).append("\n");

          if (counter.getName().equals("Tuples_Read")) anyTuplesRead = true;
          if (counter.getName().equals("Tuples_Written")) anyTuplesWritten = true;
        }
      }

      if (anyTuplesRead && !anyTuplesWritten) {
        builder.append("  *** BLACK HOLE WARNING *** The above step had input but no output\n");
      }
    }
    builder.append(StringUtils.repeat("=", 90)).append("\n");
    return builder.toString();
  }

  private static final String prettyTaps(Map<String, Tap> taps) {
    if (taps.keySet().isEmpty()) return "[]";

    Tap first = taps.values().toArray(new Tap[0])[0];
    if (first == null) return "[null tap]";

    if (taps.keySet().size() == 1) {
      return "[\"" + first.getIdentifier() + "\"]";
    } else {
      return "[\"" + first.getIdentifier() + "\",...]";
    }
  }
}
