Project cascading_ext
========
 
cascading_ext is a collection of tools built on top of the [Cascading](https://github.com/cwensel/cascading) platform which make it easy to build, debug, and run simple and high-performance data workflows. 

Features
====

Some of the most interesting public classes in the project (so far).

### SubAssemblies ###

<b>BloomJoin</b>

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

<b>BloomFilter</b>

[BloomFilter](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/assembly/BloomFilter.java) is similar to BloomJoin, but can be used when no fields from the RHS are needed in the output.  This allows for simpler field algebra (duplicate field names are not a problem):

```java
Pipe source1 = new Pipe("source1");
Pipe source2 = new Pipe("source2");

Pipe joined = new BloomFilter(source1, new Fields("field1"), source2, new Fields("field1"), true);

CascadingUtil.get().getFlowConnector().connect("Example flow", sources, sink, joined).complete();
```

Another feature of BloomFilter is the ability to perform an inexact filter, and entirely avoid reducing over the LHS.  When performing an inexact join, the LHS is filtered by the bloom filter from the RHS, but the final exact CoGroup is skipped, leaving both true and false positives in the output.   See [TestBloomFilter](https://github.com/LiveRamp/cascading_ext/blob/master/src/test/java/com/liveramp/cascading_ext/assembly/TestBloomFilter.java) for more examples.

<b>MultiGroupBy</b> 

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

<b>CascadingUtil</b> 

[CascadingUtil](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/CascadingUtil.java) is a utility class which makes it easy to add default properties and strategies to all jobs which are run in a codebase, and which adds some useful logging and debugging information.  For a simple example of how to use this class, see [SimpleFlowExample](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/example/SimpleFlowExample.java):

```java
CascadingUtil.get().getFlowConnector().connect("Example flow", sources, sink, pipe).complete();
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

<b>FunctionStats, FilterStats, BufferStats, AggregatorStats</b>

[FunctionStats](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/operation/FunctionStats.java), [FilterStats](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/operation/FilterStats.java), [BufferStats](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/operation/BufferStats.java), and [AggregatorStats](https://github.com/LiveRamp/cascading_ext/blob/master/src/main/java/com/liveramp/cascading_ext/operation/AggregatorStats.java) are wrapper classes which make it easier to debug a complex flow with many Filters / Functions / etc.  These wrapper classes add counters logging the number of tuples a given operation inputs or outputs.  See [TestAggregatorStats](https://github.com/LiveRamp/cascading_ext/blob/master/src/test/java/com/liveramp/cascading_ext/operation/TestAggregatorStats.java), [TestFilterStats](https://github.com/LiveRamp/cascading_ext/blob/master/src/test/java/com/liveramp/cascading_ext/operation/TestFilterStats.java), [TestFunctionStats](https://github.com/LiveRamp/cascading_ext/blob/master/src/test/java/com/liveramp/cascading_ext/operation/TestFunctionStats.java), and [TestBufferStats](https://github.com/LiveRamp/cascading_ext/blob/master/src/test/java/com/liveramp/cascading_ext/operation/TestBufferStats.java) to see usage examples.

Download
====
You can either build cascading_ext from source as described below, or pull the latest jar from [downloads](https://github.com/LiveRamp/cascading_ext/downloads).

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

HADOOP_HOME and HADOOP_CONF_DIR must point to your local hadoop install.

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
Copyright 2012 LiveRamp

Licensed under the Apache License, Version 2.0

http://www.apache.org/licenses/LICENSE-2.0

