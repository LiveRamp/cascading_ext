package com.liveramp.cascading_ext.flow_step_strategy;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepStrategy;
import cascading.tap.Tap;
import com.liveramp.cascading_ext.tap.NullTap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.JobConf;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RenameJobStrategy implements FlowStepStrategy<JobConf> {

  private static final int MAX_SOURCE_SINK_LENGTH = 50;
  private static final String TMP_TAP_NAME = "{tmp}";

  @Override
  public void apply(Flow<JobConf> flow, List<FlowStep<JobConf>> predecessorSteps, FlowStep<JobConf> flowStep) {
    //  Give jobs human readable names. The default naming scheme includes a bunch of randomly
    //  generated IDs.
    flowStep.getConfig().setJobName(formatJobName(flowStep));
  }

  private static String formatJobName(FlowStep<JobConf> flowStep) {
    return String.format("%s[%s%s]",
            flowStep.getFlowName(), // for example "Creating Bloom Filter"
            flowStep.getName(),     // for some unknown reason, this is set to, for example (1/6)
            formatSourcesAndSinks(flowStep)); // add in sources and sinks
  }

  private static String formatSourcesAndSinks(FlowStep<JobConf> flowStep) {
    String full = String.format("[%s][%s]",
            getTapSetString(flowStep.getSources()),
            getTapSetString(flowStep.getSinks()));

    return StringUtils.abbreviate(full, full.length() - 1, MAX_SOURCE_SINK_LENGTH);
  }

  private static String getTapSetString(Set<Tap> taps) {
    Set<String> stringIds = new HashSet<String>();

    for (Tap tap : taps) {
      if (tap instanceof NullTap) {
        stringIds.add(NullTap.class.getSimpleName());
      } else {
        stringIds.add(tap.isTemporary() ? TMP_TAP_NAME : tap.getIdentifier());
      }
    }

    return formatSetOfNames(stringIds);
  }

  private static String formatSetOfNames(Set<String> names) {
    return StringUtils.join(names, ",");
  }
}
