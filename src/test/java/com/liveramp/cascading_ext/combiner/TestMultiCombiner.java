package com.liveramp.cascading_ext.combiner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.twitter.maple.tap.MemorySourceTap;
import org.junit.Before;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.combiner.lib.SumExactAggregator;

import static org.junit.Assert.assertEquals;

public class TestMultiCombiner extends BaseTestCase {

  public static final String USER_A = "PartnerA";
  public static final String ATTRIBUTE_1 = "Destination1";
  public static final String DAY1 = "day1";
  public static final String DAY2 = "day2";
  public static final String ATTRIBUTE_2 = "Destination2";
  public static final String USER_B = "PartnerB";
  public static final String USER_C = "PartnerC";
  private MemorySourceTap source;
  private CombinerDefinition<Number[]> def1, def2, def3, def4, def5;
  private ArrayList<Tuple> expectedTuplesPerUserAttribute;
  private ArrayList<Tuple> expectedTuplesPerAttribute;
  private ArrayList<Tuple> expectedTuplesPerUser;
  private ArrayList<Tuple> expectedTupleForUserAttributeDay;
  private ArrayList<Tuple> expectedTuplesPerDay;
  private ArrayList<Tuple> allExpectedTuples;

  @Before
  public void prepare() throws Exception {
    source = new MemorySourceTap(
        Lists.newArrayList(
            new Tuple(USER_A, ATTRIBUTE_1, DAY1, 1),
            new Tuple(USER_A, ATTRIBUTE_1, DAY1, 1),
            new Tuple(USER_A, ATTRIBUTE_1, DAY1, 1),
            new Tuple(USER_A, ATTRIBUTE_1, DAY2, 1),
            new Tuple(USER_A, ATTRIBUTE_2, DAY2, 1),
            new Tuple(USER_A, ATTRIBUTE_2, DAY2, 1),
            new Tuple(USER_A, ATTRIBUTE_2, DAY1, 1),
            new Tuple(USER_B, ATTRIBUTE_1, DAY1, 1),
            new Tuple(USER_B, ATTRIBUTE_1, DAY2, 1),
            new Tuple(USER_B, ATTRIBUTE_1, DAY2, 1),
            new Tuple(USER_C, ATTRIBUTE_2, DAY2, 1)
        ),
        new Fields("partner", "destination", "date", "requests")
    );
    def1 = new CombinerDefinitionBuilder<Number[]>()
        .setExactAggregator(new SumExactAggregator(1))
        .setGroupFields(new Fields("partner"))
        .setInputFields(new Fields("requests"))
        .setOutputFields(new Fields("request-per-partner"))
        .setName("request-per-partner")
        .get();

    def2 = new CombinerDefinitionBuilder<Number[]>()
        .setExactAggregator(new SumExactAggregator(1))
        .setGroupFields(new Fields("partner", "destination"))
        .setInputFields(new Fields("requests"))
        .setOutputFields(new Fields("request-per-partner-and-destination"))
        .setName("request-per-partner-and-destination")
        .get();

    def3 = new CombinerDefinitionBuilder<Number[]>()
        .setExactAggregator(new SumExactAggregator(1))
        .setGroupFields(new Fields("partner", "destination", "date"))
        .setInputFields(new Fields("requests"))
        .setOutputFields(new Fields("request-per-partner-and-destination-by-day"))
        .setName("request-per-partner-and-destination-by-day")
        .get();

    def4 = new CombinerDefinitionBuilder<Number[]>()
        .setExactAggregator(new SumExactAggregator(1))
        .setGroupFields(new Fields("date"))
        .setInputFields(new Fields("requests"))
        .setOutputFields(new Fields("requests-per-day"))
        .setName("requests-per-day")
        .get();

    def5 = new CombinerDefinitionBuilder<Number[]>()
        .setExactAggregator(new SumExactAggregator(1))
        .setGroupFields(new Fields("destination"))
        .setInputFields(new Fields("requests"))
        .setOutputFields(new Fields("requests-by-destination"))
        .setName("requests-by-destination")
        .get();

    expectedTuplesPerUserAttribute = Lists.newArrayList(
        new Tuple(USER_A, ATTRIBUTE_1, 4l),
        new Tuple(USER_A, ATTRIBUTE_2, 3l),
        new Tuple(USER_C, ATTRIBUTE_2, 1l),
        new Tuple(USER_B, ATTRIBUTE_1, 3l));

    expectedTuplesPerAttribute = Lists.newArrayList(
        new Tuple(ATTRIBUTE_1, 7l),
        new Tuple(ATTRIBUTE_2, 4l));

    expectedTuplesPerUser = Lists.newArrayList(
        new Tuple(USER_A, 7l),
        new Tuple(USER_C, 1l),
        new Tuple(USER_B, 3l));

    expectedTupleForUserAttributeDay = Lists.newArrayList(new Tuple(USER_A, ATTRIBUTE_1, DAY1, 3l),
        new Tuple(USER_A, ATTRIBUTE_1, DAY2, 1l),
        new Tuple(USER_A, ATTRIBUTE_2, DAY1, 1l),
        new Tuple(USER_A, ATTRIBUTE_2, DAY2, 2l),
        new Tuple(USER_C, ATTRIBUTE_2, DAY2, 1l),
        new Tuple(USER_B, ATTRIBUTE_1, DAY1, 1l),
        new Tuple(USER_B, ATTRIBUTE_1, DAY2, 2l));

    expectedTuplesPerDay = Lists.newArrayList(new Tuple(DAY1, 5l),
        new Tuple(DAY2, 6l));

    allExpectedTuples = Lists.newArrayList();
    allExpectedTuples.addAll(expectedTuplesPerDay);
    allExpectedTuples.addAll(expectedTupleForUserAttributeDay);
    allExpectedTuples.addAll(expectedTuplesPerUser);
    allExpectedTuples.addAll(expectedTuplesPerAttribute);
    allExpectedTuples.addAll(expectedTuplesPerUserAttribute);
  }

  @Test
  public void testMultiCombiner() throws IOException {
    Pipe pipe = new Pipe("pipe");
    SubAssembly multiCombiner = MultiCombiner.assembly(pipe, def1, def2, def3, def4, def5);
    Pipe[] tails = multiCombiner.getTails();

    Map<String, Tap> sinks = Maps.newHashMap();

    for (CombinerDefinition def : Lists.<CombinerDefinition>newArrayList(def1, def2, def3, def4, def5)) {
      Tap output = getTupleOutputTap("testMultipleTails", def.getName(), def.getGroupFields().append(def.getOutputFields()));
      sinks.put(def.getName(), output);
    }

    Flow flow = CascadingUtil.get().getFlowConnector().connect(source, sinks, tails);
    flow.complete();
    assertEquals(6, flow.getFlowStats().getStepsCount());

    verifyRequestsPerUser(sinks.get(def1.getName()));
    verifyRequestsPerUserAttribute(sinks.get(def2.getName()));
    verifyRequestsPerUserAttributeDay(sinks.get(def3.getName()));
    verifyRequestsPerDay(sinks.get(def4.getName()));
    verifyRequestsPerAttribute(sinks.get(def5.getName()));
  }

  private Tap getTupleOutputTap(String testname, String name, Fields fields) {
    return new Hfs(new SequenceFile(fields), getTestRoot() + "/multi_combiner_output/" + testname + "/" + name);
  }

  @Test
  public void testMultiCombinerSingleTail() throws IOException {
    Pipe pipe = new Pipe("pipe");
    Pipe multiCombiner = MultiCombiner.singleTailedAssembly(pipe, def1, def2, def3, def4, def5);

    Tap output = getTupleOutputTap("testMultiCombinerSingleTail", "all",
        MultiCombiner.getOutputFields(Lists.<CombinerDefinition>newArrayList(def1, def2, def3, def4, def5)));

    Flow flow = CascadingUtil.get().getFlowConnector().connect(source, output, multiCombiner);
    flow.complete();
    assertEquals(1, flow.getFlowStats().getStepsCount());

    List<Tuple> allTuples = getAllTuples(output);
    for (int i = 0; i < allTuples.size(); i++) {
      Tuple tuple = stripNullsAndCombinerId(allTuples.get(i));
      allTuples.set(i, tuple);
    }
    assertCollectionEquivalent(allExpectedTuples, allTuples);
  }

  private Tuple stripNullsAndCombinerId(Tuple tuple) {
    List<Integer> positions = Lists.newArrayList();
    for (int i = 1; i < tuple.size(); i++) {
      if (tuple.getObject(i) != null) {
        positions.add(i);
      }
    }
    Tuple output = Tuple.size(positions.size());
    for (int i = 0; i < output.size(); i++) {
      output.set(i, tuple.getObject(positions.get(i)));
    }
    return output;
  }

  private void verifyRequestsPerDay(Tap tap) throws IOException {
    List<Tuple> allTuples = getAllTuples(tap);
    assertCollectionEquivalent(expectedTuplesPerDay, allTuples);
  }

  private void verifyRequestsPerUserAttributeDay(Tap tap) throws IOException {
    List<Tuple> allTuples = getAllTuples(tap);
    assertCollectionEquivalent(expectedTupleForUserAttributeDay, allTuples);
  }

  private void verifyRequestsPerUserAttribute(Tap tap) throws IOException {
    List<Tuple> allTuples = getAllTuples(tap);
    assertCollectionEquivalent(expectedTuplesPerUserAttribute, allTuples);
  }

  private void verifyRequestsPerUser(Tap tap) throws IOException {
    List<Tuple> allTuples = getAllTuples(tap);

    assertCollectionEquivalent(expectedTuplesPerUser, allTuples);
  }

  private void verifyRequestsPerAttribute(Tap tap) throws IOException {
    List<Tuple> allTuples = getAllTuples(tap);
    assertCollectionEquivalent(expectedTuplesPerAttribute, allTuples);
  }
}
