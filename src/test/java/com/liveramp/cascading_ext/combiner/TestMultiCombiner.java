package com.liveramp.cascading_ext.combiner;

import cascading.flow.Flow;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.liveramp.cascading_ext.BaseTestCase;
import com.liveramp.cascading_ext.CascadingUtil;
import com.liveramp.cascading_ext.combiner.lib.SumExactAggregator;
import com.twitter.maple.tap.MemorySourceTap;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestMultiCombiner extends BaseTestCase {

  public static final String PARTNER_A = "PartnerA";
  public static final String DESTINATION_1 = "Destination1";
  public static final String DAY1 = "day1";
  public static final String DAY2 = "day2";
  public static final String DESTINATION_2 = "Destination2";
  public static final String PARTNER_B = "PartnerB";
  public static final String PARTNER_C = "PartnerC";
  private MemorySourceTap source;
  private CombinerDefinition<Number[]> def1, def2, def3, def4, def5;
  private ArrayList<Tuple> expectedTuplesPerPartnerDestination;
  private ArrayList<Tuple> expectedTuplesPerDestination;
  private ArrayList<Tuple> expectedTuplesPerCustomer;
  private ArrayList<Tuple> expectedTupleForPartnerDestinationDay;
  private ArrayList<Tuple> expectedTuplesPerDay;
  private ArrayList<Tuple> allExpectedTuples;

  @Before
  public void prepare() throws Exception {
    source = new MemorySourceTap(
        Lists.<Tuple>newArrayList(
            new Tuple(PARTNER_A, DESTINATION_1, DAY1, 1),
            new Tuple(PARTNER_A, DESTINATION_1, DAY1, 1),
            new Tuple(PARTNER_A, DESTINATION_1, DAY1, 1),
            new Tuple(PARTNER_A, DESTINATION_1, DAY2, 1),
            new Tuple(PARTNER_A, DESTINATION_2, DAY2, 1),
            new Tuple(PARTNER_A, DESTINATION_2, DAY2, 1),
            new Tuple(PARTNER_A, DESTINATION_2, DAY1, 1),
            new Tuple(PARTNER_B, DESTINATION_1, DAY1, 1),
            new Tuple(PARTNER_B, DESTINATION_1, DAY2, 1),
            new Tuple(PARTNER_B, DESTINATION_1, DAY2, 1),
            new Tuple(PARTNER_C, DESTINATION_2, DAY2, 1)
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

    expectedTuplesPerPartnerDestination = Lists.newArrayList(
        new Tuple(PARTNER_A, DESTINATION_1, 4l),
        new Tuple(PARTNER_A, DESTINATION_2, 3l),
        new Tuple(PARTNER_C, DESTINATION_2, 1l),
        new Tuple(PARTNER_B, DESTINATION_1, 3l));

    expectedTuplesPerDestination = Lists.newArrayList(
        new Tuple(DESTINATION_1, 7l),
        new Tuple(DESTINATION_2, 4l));

    expectedTuplesPerCustomer = Lists.newArrayList(
        new Tuple(PARTNER_A, 7l),
        new Tuple(PARTNER_C, 1l),
        new Tuple(PARTNER_B, 3l));

    expectedTupleForPartnerDestinationDay = Lists.newArrayList(new Tuple(PARTNER_A, DESTINATION_1, DAY1, 3l),
        new Tuple(PARTNER_A, DESTINATION_1, DAY2, 1l),
        new Tuple(PARTNER_A, DESTINATION_2, DAY1, 1l),
        new Tuple(PARTNER_A, DESTINATION_2, DAY2, 2l),
        new Tuple(PARTNER_C, DESTINATION_2, DAY2, 1l),
        new Tuple(PARTNER_B, DESTINATION_1, DAY1, 1l),
        new Tuple(PARTNER_B, DESTINATION_1, DAY2, 2l));

    expectedTuplesPerDay = Lists.newArrayList(new Tuple(DAY1, 5l),
        new Tuple(DAY2, 6l));

    allExpectedTuples = Lists.newArrayList();
    allExpectedTuples.addAll(expectedTuplesPerDay);
    allExpectedTuples.addAll(expectedTupleForPartnerDestinationDay);
    allExpectedTuples.addAll(expectedTuplesPerCustomer);
    allExpectedTuples.addAll(expectedTuplesPerDestination);
    allExpectedTuples.addAll(expectedTuplesPerPartnerDestination);
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

    verifyRequestsPerCustomer(sinks.get(def1.getName()));
    verifyRequestsPerCustomerAdn(sinks.get(def2.getName()));
    verifyRequestsPerCustomerAdnDay(sinks.get(def3.getName()));
    verifyRequestsPerDay(sinks.get(def4.getName()));
    verifyRequestsPerDestination(sinks.get(def5.getName()));
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

  private void verifyRequestsPerCustomerAdnDay(Tap tap) throws IOException {
    List<Tuple> allTuples = getAllTuples(tap);
    assertCollectionEquivalent(expectedTupleForPartnerDestinationDay, allTuples);
  }

  private void verifyRequestsPerCustomerAdn(Tap tap) throws IOException {
    List<Tuple> allTuples = getAllTuples(tap);
    assertCollectionEquivalent(expectedTuplesPerPartnerDestination, allTuples);
  }

  private void verifyRequestsPerCustomer(Tap tap) throws IOException {
    List<Tuple> allTuples = getAllTuples(tap);

    assertCollectionEquivalent(expectedTuplesPerCustomer, allTuples);
  }

  private void verifyRequestsPerDestination(Tap tap) throws IOException {
    List<Tuple> allTuples = getAllTuples(tap);
    assertCollectionEquivalent(expectedTuplesPerDestination, allTuples);
  }
}
