package com.liveramp.cascading_ext.flow_step_strategy;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepStrategy;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import com.liveramp.cascading_ext.tap.NullTap;
import com.twitter.maple.tap.MemorySourceTap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.JobConf;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RenameJobStrategy implements FlowStepStrategy<JobConf> {

  private static final int MAX_SOURCE_OR_SINK_LENGTH = 200;
  private static final String TMP_TAP_NAME = "{tmp}";
  private static final Pattern TEMP_PIPE_NAME = Pattern.compile("(/.*?)+/(.*?_\\d+_[A-Z0-9]{32})$");

  @Override
  public void apply(Flow<JobConf> flow, List<FlowStep<JobConf>> predecessorSteps, FlowStep<JobConf> flowStep) {
    //  Give jobs human readable names. The default naming scheme includes a bunch of randomly generated IDs.
    flowStep.getConfig().setJobName(formatJobName(flowStep));
  }

  protected String formatJobName(FlowStep<JobConf> flowStep) {
    return String.format("%s[%s%s]",
        flowStep.getFlowName(),
        getStepNumber(flowStep),
        formatSourcesAndSinks(flowStep));
  }

  protected String getStepNumber(FlowStep<JobConf> flowStep){
    return "(" + flowStep.getStepNum() + "/" + flowStep.getFlow().getFlowSteps().size() + ")";
  }

  protected String formatSourcesAndSinks(FlowStep<JobConf> flowStep) {
    return String.format("[%s]=>[%s]",
        getTapSetString(flowStep.getSources()),
        getTapSetString(flowStep.getSinks()));
  }

  protected String getTapSetString(Set<Tap> taps) {
    Set<String> stringIds = new HashSet<String>();

    for (Tap tap : taps) {
      stringIds.add(getTapIdentifier(tap));
    }
    String tapSet = formatSetOfNames(stringIds);
    return StringUtils.abbreviate(tapSet, tapSet.length(), MAX_SOURCE_OR_SINK_LENGTH);
  }

  /**
   * Subclasses can override this and call super() to add handling for other unrecognized classes
   *
   * @param tap
   * @return
   */
  protected String getTapIdentifier(Tap tap){

    //  MemorySourceTap and NullTap both have  really annoying random identifiers that aren't important to note
    if (tap instanceof NullTap) {
      return NullTap.class.getSimpleName();
    } else if (tap instanceof MemorySourceTap) {
      return MemorySourceTap.class.getSimpleName();
    }

    //  concatenate all sources in a multi source tap
    else if (tap instanceof MultiSourceTap) {
      MultiSourceTap multi = (MultiSourceTap) tap;
      List<String> sources = new ArrayList<String>();
      Iterator children = multi.getChildTaps();
      while (children.hasNext()) {
        Object t = children.next();
        if(t instanceof Tap){
          sources.add(getTapIdentifier((Tap) t));
        }
      }
      return StringUtils.join(sources, "+");
    }

    // add the pipe name for temporary taps
    else if (tap.isTemporary()) {
      String tmpDir = tap.getIdentifier();
      Matcher m = TEMP_PIPE_NAME.matcher(tmpDir);
      m.matches();
      if (m.groupCount() > 1) {
        return "pipe:" + m.group(m.groupCount());
      } else {
        return TMP_TAP_NAME;
      }
    }

    //  otherwise hope the identifier is useful
    else {
      return tap.getIdentifier();
    }
  }

  protected String formatSetOfNames(Set<String> names) {
    return StringUtils.join(names, ",");
  }
}
