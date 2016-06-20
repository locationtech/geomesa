Accumulo Data Store
===================

The data store module contains all of the Accumulo-related code for GeoMesa. This includes client code and distributed iterator code for the Accumulo tablet servers.

Installation
------------

Use of the Accumulo data store requires that the distributed runtime JAR be installed on the tablet servers, described in more detail in :ref:`install_accumulo_runtime`.

Creating a Data Store
---------------------

An instance of an Accumulo data store can be obtained through the normal GeoTools discovery methods, assuming that the GeoMesa code is on the classpath:

.. code-block:: java

    Map<String, String> parameters = new HashMap<>;
    parameters.put("instanceId", "myInstance");
    parameters.put("zookeepers", "zoo1,zoo2,zoo3");
    parameters.put("user", "myUser");
    parameters.put("password", "myPassword");
    parameters.put("tableName", "my_table");
    org.geotools.data.DataStore dataStore = org.geotools.data.DataStoreFinder.getDataStore(parameters);

More information on using GeoTools can be found in the `GeoTools user guide <http://docs.geotools.org/stable/userguide/>`_.

Using the Accumulo Data Store in GeoServer
------------------------------------------

See :doc:`./geoserver`.

Indexing Strategies
-------------------

GeoMesa uses several different strategies to index simple features. In the code, these strategies are abstracted as 'tables'. For details on how GeoMesa encodes and indexes data, see tables. For details on how GeoMesa chooses and executes queries, see the ``org.locationtech.geomesa.accumulo.index.QueryPlanner`` and ``org.locationtech.geomesa.accumulo.index.QueryStrategyDecider`` classes.

Explaining:  Query Plans
------------------------

Given a data store and a query, you can ask GeoMesa to explain its plan for how to execute the query:

.. code-block:: java

    dataStore.getQueryPlan(query, explainer = new ExplainPrintln);

Instead of ``ExplainPrintln``, you can also use ``ExplainString`` or ``ExplainLogging`` to redirect the explainer output elsewhere.  (For the ``ExplainLogging``, it may be helpful to refer to GeoServer's `Advanced log configuration <http://docs.geoserver.org/2.8.x/en/user/advanced/logging.html>`_ documentation for the specifics of how and where to manage the GeoServer logs.)

Knowing the plan -- including information such as the indexing strategy -- can be useful when you need to debug slow queries.  It can suggest when indexes should be added as well as when query-hints may expedite execution times.

Iterator Stack
--------------

GeoMesa uses Accumulo iterators to push processing out to the whole cluster. The iterator stack can be considered a 'tight inner loop' - generally, every feature returned will be processed in the iterators. As such, the iterators have been written for performance over readability.

We use several techniques to improve iterator performance. For one, we only deserialize the attributes of a simple feature that we need to evaluate a given query. When retrieving attributes, we always look them up by index, instead of by name. For aggregating queries, we create partial aggregates in the iterators, instead of doing all the processing in the client. The main goals are to minimize disk reads, processing and bandwidth as much as possible.

For more details, see the ``org.locationtech.geomesa.accumulo.iterators`` package.

Configuration
-------------

Zookeeper session timeout
~~~~~~~~~~~~~~~~~~~~~~~~~

The `Zookeeper session timeout <http://accumulo.apache.org/1.6/accumulo_user_manual#_instance_zookeeper_timeout>`__
for the GeoMesa Accumulo data store is exposed as the Java system property ``instance.zookeeper.timeout``:

.. code-block:: bash

    export JAVA_OPTS="-Dinstance.zookeeper.timeout=10s"
