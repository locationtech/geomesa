Internals
=========

Indexing Strategies
-------------------

For details on how GeoMesa chooses and executes queries, see the `QueryPlanner`_ and
`StrategyDecider`_ classes in the **geomesa-index-api** project. The generic query
planning API is configured for the Accumulo data store in the `AccumuloQueryPlanner`_ class.

.. _QueryPlanner: https://github.com/locationtech/geomesa/blob/master/geomesa-index-api/src/main/scala/org/locationtech/geomesa/index/planning/QueryPlanner.scala

.. _StrategyDecider: https://github.com/locationtech/geomesa/blob/master/geomesa-index-api/src/main/scala/org/locationtech/geomesa/index/api/StrategyDecider.scala

.. _AccumuloQueryPlanner: https://github.com/locationtech/geomesa/blob/master/geomesa-accumulo/geomesa-accumulo-datastore/src/main/scala/org/locationtech/geomesa/accumulo/index/AccumuloQueryPlanner.scala

Iterator Stack
--------------

GeoMesa uses Accumulo iterators to push processing out to the whole cluster. The iterator stack can be considered a 'tight inner loop' - generally, every feature returned will be processed in the iterators. As such, the iterators have been written for performance over readability.

We use several techniques to improve iterator performance. For one, we only deserialize the attributes of a simple feature that we need to evaluate a given query. When retrieving attributes, we always look them up by index, instead of by name. For aggregating queries, we create partial aggregates in the iterators, instead of doing all the processing in the client. The main goals are to minimize disk reads, processing and bandwidth as much as possible.

For more details, see the `org.locationtech.geomesa.accumulo.iterators <https://github.com/locationtech/geomesa/tree/master/geomesa-accumulo/geomesa-accumulo-datastore/src/main/scala/org/locationtech/geomesa/accumulo/iterators>`__ package.
