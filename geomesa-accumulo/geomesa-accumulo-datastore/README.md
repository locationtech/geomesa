# GeoMesa Accumulo Data Store

The data store module contains all of the Accumulo-related code for GeoMesa. This includes client code and
distributed iterator code for the Accumulo tablet servers.

### Accumulo Data Store

An instance of an Accumulo data store can be obtained through the normal GeoTools discovery methods, assuming
that the GeoMesa code is on the classpath:

```java
Map<String, String> parameters = new HashMap<>;
parameters.put("instanceId", "myInstance");
parameters.put("zookeepers", "zoo1,zoo2,zoo3");
parameters.put("user", "myUser");
parameters.put("password", "myPassword");
parameters.put("tableName", "my_table");
org.geotools.data.DataStore dataStore = org.geotools.data.DataStoreFinder.getDataStore(parameters);
```

More information on using GeoTools can be found in the GeoTools [user guide](http://docs.geotools.org/stable/userguide/).

### Indexing Strategies

GeoMesa uses several different strategies to index simple features. In the code, these strategies are
abstracted as 'tables'. For details on how GeoMesa encodes and indexes data, see
[tables](src/main/scala/org/locationtech/geomesa/accumulo/data/tables). For details on how GeoMesa
chooses and executes queries, see the [QueryPlanner](src/main/scala/org/locationtech/geomesa/accumulo/index/QueryPlanner.scala)
and [QueryStrategyDecider](src/main/scala/org/locationtech/geomesa/accumulo/index/QueryStrategyDecider.scala).

### Iterator Stack

GeoMesa uses Accumulo iterators to push processing out to the whole cluster. The iterator stack can be considered
a 'tight inner loop' - generally, every feature returned will be processed in the iterators. As such,
the iterators have been written for performance over readability.

We use several techniques to improve iterator performance. For one, we only deserialize the attributes of a
simple feature that we need to evaluate a given query. When retrieving attributes, we always look them up
by index, instead of by name. For aggregating queries, we create partial aggregates in the iterators, instead
of doing all the processing in the client. The main goals are to minimize disk reads, processing and bandwidth
as much as possible.

For more details, see [iterators](src/main/scala/org/locationtech/geomesa/accumulo/iterators).