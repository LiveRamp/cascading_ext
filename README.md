Project cascading_ext
========
       
cascading_ext is a collection of tools built on top of the [Cascading](https://github.com/cwensel/cascading) platform which make it easy to build, debug, and run simple and high-performance data workflows. 
   
Features
====
 
Some of the most interesting public classes in the project (so far).

### SubAssemblies ###

##### BloomJoin

[BloomJoin](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/assembly/BloomJoin.java) is designed to be a drop-in replacement for CoGroup which achieves significant performance improvements on certain datasets by filtering the LHS pipe against a [bloom filter](http://en.wikipedia.org/wiki/Bloom_filter) built from the keys on the RHS.  Using a BloomJoin can improve the performance of a job when:

- joining a large LHS store against a relatively small RHS store
- most reduce input data does not make it into the output store
- the job is a good candidate for HashJoin, but the RHS tuples don't fit in memory

Internally, we have cut the reduce time of jobs by up to 90% when a BloomJoin lets the job only reduce over the small subset of the data that makes it past the bloom filter.

The constructor signature mirrors CoGroup:

```java
Pipe source1 = new Pipe("source1");
Pipe source2 = new Pipe("source2");

Pipe joined = new BloomJoin(source1, new Fields("field1"), source2, new Fields("field3"));

CascadingUtil.get().getFlowConnector().connect("Example flow", sources, sink, joined).complete();
```

see example usages: [BloomJoinExample](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/example/BloomJoinExample.java), [BloomJoinExampleWithoutCascadingUtil](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/example/BloomJoinExampleWithoutCascadingUtil.java)

For more details on how BloomJoin works, check out our [blog post](http://blog.liveramp.com/2013/04/03/bloomjoin-bloomfilter-cogroup/).

##### BloomFilter

[BloomFilter](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/assembly/BloomFilter.java) is similar to BloomJoin, but can be used when no fields from the RHS are needed in the output.  This allows for simpler field algebra (duplicate field names are not a problem):

```java
Pipe source1 = new Pipe("source1");
Pipe source2 = new Pipe("source2");

Pipe joined = new BloomFilter(source1, new Fields("field1"), source2, new Fields("field1"), true);

CascadingUtil.get().getFlowConnector().connect("Example flow", sources, sink, joined).complete();
```

Another feature of BloomFilter is the ability to perform an inexact filter, and entirely avoid reducing over the LHS.  When performing an inexact join, the LHS is filtered by the bloom filter from the RHS, but the final exact CoGroup is skipped, leaving both true and false positives in the output.   See [TestBloomFilter](https://github.com/LiveRamp/cascading_ext/blob/master/src/test/java/com/liveramp/cascading_ext/assembly/TestBloomFilter.java) for more examples.

##### MultiGroupBy

[MultiGroupBy](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/assembly/MultiGroupBy.java) allows the user to easily GroupBy two or more pipes on a common field without performing a full Inner/OuterJoin first (which can lead to an explosion in the number of tuples, if keys are not distinct.)  The [MultiBuffer](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/multi_group_by/MultiBuffer.java) interface gives a user-defined function access to all tuples sharing a common key, across all input pipes:  

```java
Pipe s1 = new Pipe("s1");
Pipe s2 = new Pipe("s2");

Pipe results = new MultiGroupBy(s1, new Fields("key"), s2, new Fields("key"),
    new Fields("key-rename"), new CustomBuffer());

CascadingUtil.get().getFlowConnector().connect(sources, sink, results).complete();
```

see [TestMultiGroupBy](https://github.com/LiveRamp/cascading_ext/blob/master/src/test/java/com/liveramp/cascading_ext/assembly/TestMultiGroupBy.java) for example usage.

### Tools ###

##### CascadingUtil 
[CascadingUtil](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/CascadingUtil.java) is a utility class which makes it easy to add default properties and strategies to all jobs which are run in a codebase, and which adds some useful logging and debugging information.  For a simple example of how to use this class, see [SimpleFlowExample](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/example/SimpleFlowExample.java):

```java
CascadingUtil.get().getFlowConnector()
.connect("Example flow", sources, sink, pipe).complete();
```

By default CascadingUtil will
  - print and format counters for each step
  - retrieve and log map or reduce task errors if the job fails
  - extend Cascading's job naming scheme with improved naming for some taps which use UUID identifiers.

See [FlowWithCustomCascadingUtil](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/example/FlowWithCustomCascadingUtil.java) to see examples of how CascadingUtil can be extended to include custom default properties.  Subclasses can easily: 

  - set default properties in the job Configuration
  - add serialization classes
  - add serialization tokens
  - add flow step strategies

###### FunctionStats, FilterStats, BufferStats, AggregatorStats

[FunctionStats](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/operation/FunctionStats.java), [FilterStats](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/operation/FilterStats.java), [BufferStats](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/operation/BufferStats.java), and [AggregatorStats](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/operation/AggregatorStats.java) are wrapper classes which make it easier to debug a complex flow with many Filters / Functions / etc.  These wrapper classes add counters logging the number of tuples a given operation inputs or outputs.  See [TestAggregatorStats](https://github.com/LiveRamp/cascading_ext/blob/master/src/test/java/com/liveramp/cascading_ext/operation/TestAggregatorStats.java), [TestFilterStats](https://github.com/LiveRamp/cascading_ext/blob/master/src/test/java/com/liveramp/cascading_ext/operation/TestFilterStats.java), [TestFunctionStats](https://github.com/LiveRamp/cascading_ext/blob/master/src/test/java/com/liveramp/cascading_ext/operation/TestFunctionStats.java), and [TestBufferStats](https://github.com/LiveRamp/cascading_ext/blob/master/src/test/java/com/liveramp/cascading_ext/operation/TestBufferStats.java) to see usage examples.

##### MultiCombiner

MultiCombiner is a tool for running arbitrarily many aggregation operations, each with their own distinct grouping fields, over a single stream of tuples using only a single reduce step. MultiCombiner uses combiners internally to ensure that these aggregation operations are done efficiently and without unnecessary I/O operations.

Combiners are a useful tool for optimizing Hadoop jobs by reducing the number of records which need to be shuffled, decreasing sorting time, disk I/O, and network traffic on your cluster. Combiners work by partially reducing on records during the map phase which have the same key, and emitting the partial result rather than the original records, in essence doing some of the work of the reducer ahead of time. Combiners decrease the number of records generated by each map task, reducing the amount of data that needs to be shuffled and often providing a significant decrease in time spent on I/O. Because each combiner only has access to a subset of the records for each key in a random order, combiners are only useful for operations which are both commutative and associative, such as summing or counting. For a quick combiner tutorial check out [this combiner tutorial](http://www.philippeadjiman.com/blog/2010/01/14/hadoop-tutorial-series-issue-4-to-use-or-not-to-use-a-combiner/).

Combiners are an integral part of Hadoop, and are supported by Cascading through AggregateBy and the Functor and Aggregator interfaces, which implement the combiner and reducer logic respectively. What Cascading's and Hadoop's implementations both lack is a way to run many combiner-backed aggregation operations over a single stream using different grouping fields in an efficient way. In both Hadoop and Cascading, such a computation would require a full read and shuffle of the data set for each of the different grouping fields. This can make it prohibitively expensive to add new stats or aggregates to a workflow. The MultiCombiner tool allows developers to define many entirely independent aggregation operations over the same data set and run them simultaneously, even if they use different grouping keys.

<b> An Example Usage </b> 

Suppose that it is your job to compute stats for a large retailer. The retailer stores records of purchase events in an HDFS file with the following format:
    user_id , item_id, item_amount, timestamp
You are required to compute

1) The total number of items purchased by each user

2) The total number of items of each kind sold

Using the features built into Cascading, we might build the flow this way:

```java
Pipe purchaseEvents = new Pipe("purchase_events");

Pipe countByUser = new SumBy( purchaseEvents , new Fields("user_id"), 
  new Fields("item_amount"), new Fields("total_items_by_user"));

Pipe countByItem = new SumBy( purchaseEvents , new Fields("item_id"), 
  new Fields("item_amount"), new Fields("total_items_by_item"));
```

Because each SumBy contains a GroupBy, this assembly will spawn 2 entirely separate Hadoop jobs, each running over the same data set. While the combiners used will mitigate the damage somewhat, your task is still reading, shuffling, and writing the same data twice. Using MultiCombiner, we can combine these two aggregation operations into a single Hadoop job, significantly reducing the total amount of I/O we have to do.

```java
Pipe purchaseEvents = new Pipe("purchase_events");

CombinerDefinition countByUserDef = new CombinerDefinitionBuilder()
        .setGroupFields(new Fields("user_id"))
        .setInputFields(new Fields("item_amount"))
        .setOutputFields(new Fields("total_items_by_user"))
        .setExactAggregator(new SumExactAggregator(1))
        .get();
        
CombinerDefinition countByItemDef = new CombinerDefinitionBuilder()
        .setGroupFields(new Fields("item_id"))
        .setInputFields(new Fields("item_amount"))
        .setOutputFields(new Fields("total_items_by_item"))
        .setExactAggregator(new SumExactAggregator(1))
        .get();

MultiCombiner  combiner = MultiCombiner.assembly(purchaseEvents, 
  countByItemDef, countByUserDef);

Pipe countsByUser = combiner.getTailsByName().get(countByUserDef.getName())
```

The tails of the MultiCombiner assembly will give access to the results for each definition separately. The tail pipes have the same name as the definitions, so you can use the definition names as keys to the map provided by the getTailsByName method.  Alternatively, there’s MultiCombiner.singleTailedAssembly which will emit the results in a single stream.

<b>Things to Watch Out For</b>
<p>The ability to run arbitrary aggregators over a single stream is pretty useful, but there’s nothing magical going on in the background. Each aggregator used in a  MultiCombiner emits roughly the same number of tuples as if it were being used on its own. Because the sorting involved in the shuffle is an n*log(n) operation, shuffling the output of many aggregators all at once is less efficient than shuffling their outputs separately. This is usually not an issue because of the time saved reading the data set only once, but may matter if the number of tuples being shuffled is much larger than the data being read from disk. Additionally, each aggregator must keep its own map in memory for its combiner. Because of the additional memory pressure, combining for many aggregators can potentially be less efficient. All of that being said, we've seen significant performance increases for every set of aggregation operations we've merged using this tool.

Download
====
You can either build cascading_ext from source as described below, or pull the latest jar from the Liveramp Maven repository:

```xml

<repository>
  <id>repository.liveramp.com</id>
  <name>liveramp-repositories</name>
  <url>http://repository.liveramp.com/artifactory/liveramp-repositories</url>
</repository>
```

Version 0.1 is built off of Cloudera Hadoop 3, (CDH3u3).  The current snapshot version (1.6) is built against CDH4.1.2.  Both are available via Maven:

```xml

<dependency>
    <groupId>com.liveramp</groupId>
    <artifactId>cascading_ext</artifactId>
    <version>0.1</version>
</dependency>

<dependency>
    <groupId>com.liveramp</groupId>
    <artifactId>cascading_ext</artifactId>
    <version>1.6-SNAPSHOT</version>
</dependency>
```

Building
====  

To build cascading_ext.jar from source,

```bash
> mvn package
```

will generate build/cascading_ext.jar.  To run the test suite locally, 

```bash
> mvn test
```

Usage
====

See usage instructions [here](https://github.com/cwensel/cascading/blob/wip-2.1/README.md) for running Cascading with Apache Hadoop.  Everything should work fine if cascading_ext.jar and all third-party jars in lib/ are in your jobjar.

To try out any of the code in the com.liveramp.cascading_ext.example package in production, a jobjar task for cascading_ext itself is available:

```bash
> mvn assembly:single
```

Bugs, features, pull requests
====

Bug reports or feature requests are welcome: https://github.com/liveramp/cascading_ext/issues

Changes you'd like us to merge in?  We love [pull requests](https://github.com/LiveRamp/cascading_ext/pulls).

Contributors
====

Most of the code here has been moved from our internal repositories so much of the original authorship has been lost in the git history.  Contributors include:

- [Andre Rodriguez](https://github.com/andrerodriguez)
- [Ben Pastel](https://github.com/benpastel)
- [Ben Podgursky](https://github.com/bpodgursky)
- [Bryan Duxbury](https://github.com/bryanduxbury)
- [Chris Mullins](https://github.com/sidoh)
- [Diane Yap](https://github.com/dianey)
- [Eddie Siegel](https://github.com/eddiesiegel)
- [Emily Leathers](https://github.com/eleather)
- [Nathan Marz](https://github.com/nathanmarz)
- [Piotr Kozikowski](https://github.com/pkozikow)
- [Porter Westling](https://github.com/pwestling)
- [Sean Carr](https://github.com/scarr2508)
- [Takashi Yonebayashi](https://github.com/takashiyonebayashi)
- [Thomas Kielbus](https://github.com/thomas-kielbus)

License
====
Copyright 2013 LiveRamp

Licensed under the Apache License, Version 2.0

http://www.apache.org/licenses/LICENSE-2.0

