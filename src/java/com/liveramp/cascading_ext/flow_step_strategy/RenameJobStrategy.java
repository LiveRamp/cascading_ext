package com.liveramp.cascading_ext.flow_step_strategy;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepStrategy;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import com.liveramp.cascading_ext.tap.NullTap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.JobConf;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Iterator;

public class RenameJobStrategy implements FlowStepStrategy<JobConf> {

  private static final int MAX_SOURCE_SINK_LENGTH = 200;
  private static final String TMP_TAP_NAME = "{tmp}";
  private static final Pattern TEMP_PIPE_NAME = Pattern.compile("(/.*?)+/(.*?)_\\d+_[A-Z0-9]{32}$");

  @Override
  public void apply(Flow<JobConf> flow, List<FlowStep<JobConf>> predecessorSteps, FlowStep<JobConf> flowStep) {
    //  Give jobs human readable names. The default naming scheme includes a bunch of randomly
    //  generated IDs.
    flowStep.getConfig().setJobName(formatJobName(flowStep));
  }

  private static String formatJobName(FlowStep<JobConf> flowStep) {
    return String.format("%s[%s%s]",
            flowStep.getFlowName(), // for example "Creating Bloom Filter"
            "("+flowStep.getStepNum()+"/"+flowStep.getFlow().getFlowSteps().size()+")", // for some unknown reason, this is set to, for example (1/6)
            formatSourcesAndSinks(flowStep)); // add in sources and sinks
  }

  private static String formatSourcesAndSinks(FlowStep<JobConf> flowStep) {
    String full = String.format("[%s]=>[%s]",
            getTapSetString(flowStep.getSources()),
            getTapSetString(flowStep.getSinks()));
    return StringUtils.abbreviate(full, full.length() - 1, MAX_SOURCE_SINK_LENGTH);
  }

  private static String getTapSetString(Set<Tap> taps) {
    Set<String> stringIds = new HashSet<String>();

    for (Tap tap : taps) {
      if (tap instanceof NullTap) {
        stringIds.add(NullTap.class.getSimpleName());
      } else if(tap instanceof MultiSourceTap) {
        MultiSourceTap multi = (MultiSourceTap) tap;
        List<String> sources = new ArrayList<String>();
        Iterator<Tap> children = multi.getChildTaps();
        while(children.hasNext()){
          Tap t = children.next();
          sources.add(t.getIdentifier());
        }
        stringIds.add(StringUtils.join(sources, "+"));
      }
      else {
        if(tap.isTemporary()){
          String tmpDir = tap.getIdentifier();
          Matcher m = TEMP_PIPE_NAME.matcher(tmpDir);
          m.matches();
          if(m.groupCount() > 1 ){
            stringIds.add("pipe:"+m.group(m.groupCount()));
          }else{
            stringIds.add(TMP_TAP_NAME);
          }
        }else{
          stringIds.add(tap.getIdentifier());
        }
      }
    }
    return formatSetOfNames(stringIds);
  }

  private static String formatSetOfNames(Set<String> names) {
    return StringUtils.join(names, ",");
  }
}
