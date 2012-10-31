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

package com.liveramp.cascading_ext.flow_step_strategy;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepStrategy;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import com.google.common.base.Joiner;
import com.liveramp.cascading_ext.tap.NullTap;
import com.twitter.maple.tap.MemorySourceTap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.JobConf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RenameJobStrategy implements FlowStepStrategy<JobConf> {
  private static final int MAX_JOB_NAME_LENGTH = 175;
  private static final int MAX_SOURCE_PATH_NAMES_LENGTH = 75;
  private static final Pattern TEMP_PIPE_NAME = Pattern.compile("^(.*?)_\\d+_[A-Z0-9]{32}$");

  @Override
  public void apply(Flow<JobConf> flow, List<FlowStep<JobConf>> predecessorSteps, FlowStep<JobConf> flowStep) {
    //  Give jobs human readable names. The default naming scheme includes a bunch of randomly
    //  generated IDs.
    flowStep.getConfig().setJobName(formatJobName(flowStep));
  }

  private static String formatJobName(FlowStep<JobConf> flowStep) {
    // WordCount [(2/5) input_1, input_2] -> output_1232_ABCDEF123456789...

    String jobName = String.format(
        "%s [(%d/%d) %s] -> %s",
        flowStep.getFlowName(),
        flowStep.getStepNum(),
        flowStep.getFlow().getFlowSteps().size(),
        StringUtils.abbreviate(
            join(getPrettyNamesForTaps(flowStep.getSources(), true)),
            MAX_SOURCE_PATH_NAMES_LENGTH),
        join(getPrettyNamesForTaps(flowStep.getSinks(), false)));

    return StringUtils.abbreviate(jobName, MAX_JOB_NAME_LENGTH);
  }

  private static List<String> getPrettyNamesForTaps(Set<Tap> taps, boolean removeRandomSuffixFromTempTaps) {
    List<String> prettyNames = new ArrayList<String>();

    for (Tap tap : taps) {
      if (tap instanceof NullTap || tap instanceof MemorySourceTap) {
        // MemorySourceTap and NullTap both have really annoying random identifiers that aren't important to note
        prettyNames.add(tap.getClass().getSimpleName());
      } else if (tap instanceof MultiSourceTap) {
        // concatenate all sources in a multi source tap
        Iterator children = ((MultiSourceTap) tap).getChildTaps();
        while (children.hasNext()) {
          Object object = children.next();
          if (object instanceof Tap) {
            prettyNames.add(getPrettyNameForTap((Tap) object, removeRandomSuffixFromTempTaps));
          }
        }
      } else {
        prettyNames.add(getPrettyNameForTap(tap, removeRandomSuffixFromTempTaps));
      }
    }

    return prettyNames;
  }

  private static String getPrettyNameForTap(Tap tap, boolean removeRandomSuffixFromTempTaps) {
    String id = tap.getIdentifier();
    if (id == null) {
      id = "null";
    }

    final String[] tokens = id.split("/");

    // use the last token as the pretty name (works well for things that
    // are paths and doesn't break things that are not)
    String prettyName = tokens[tokens.length-1];

    // For temporary sources, we don't care about the random suffix appended by cascading
    if (tap.isTemporary() && removeRandomSuffixFromTempTaps) {
      prettyName = getCanonicalName(prettyName);
    }

    if (prettyName.matches("^\\d+$")) {
      // For versioned stores, return "store_name/version_number", instead of just "version_number"
      if (tokens.length > 1) {
        return tokens[tokens.length-2] + "/" + prettyName;
      } else {
        return prettyName;
      }
    } else {
      return prettyName;
    }
  }

  private static String join(List<String> strings) {
    return Joiner.on(", ").join(strings);
  }

  private static String getCanonicalName(String name) {
    Matcher matcher = TEMP_PIPE_NAME.matcher(name);

    if (matcher.matches()) {
      return matcher.group(1);
    } else {
      return name;
    }
  }
}
