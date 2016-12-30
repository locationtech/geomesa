GeoMesa Spark
=============

GeoMesa Spark allows for execution of jobs on Apache Spark using data stored in GeoMesa,
other GeoTools ``DataStore``s, or files readable by the GeoMesa converter library.
The library allows creation of Spark ``RDD``s and ``DataFrame``s, writing of
Spark ``RDD``s and ``DataFrame``s to GeoMesa Accumulo and other GeoTools ``DataStore``s, and serialization of ``SimpleFeature``s using Kryo.

Example
-------

.. short example?

Architecture
------------

The ``geomesa-spark-core`` module provides the core of GeoMesa's Spark support,
through the ``GeoMesaSpark`` object...

.. spark-sql
.. include architecture diagram(s) from the Jupyter powerpoint?

Spark Core
----------

The module ``geomesa-spark-core`` provides an API for accessing geospatial data
in Spark, by defining an interface called ``SpatialRDDProvider``. Different
implementations of this interface connect to GeoMesa Accumulo, generic
GeoTools-based ``DataStore``s, or data files in formats readable by the GeoMesa
converter library. ``GeoMesaSpark`` loads an approprate ``SpatialRDDProvider``
implementation via the Java Service Provider Interface when those implementations
are added to the classpath.

Accumulo RDD
^^^^^^^^^^^^

``AccumuloSpatialRDDProvider`` is provided by the ``geomesa-accumulo-spark`` module:

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-accumulo-spark_2.11</artifactId>
      // version, etc.
    </dependency>

When given configuration parameters to connect to a GeoMesa ``AccumuloDataStore``...

.. introduction
.. parameters/configuration for accumulo

Converter RDD
^^^^^^^^^^^^^

``ConverterSpatialRDDProvider`` is provided by the ``geomesa-spark-converter`` module:

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-spark-converter_2.11</artifactId>
      // version, etc.
    </dependency>

.. introduction
.. parameters/configuration for converters
.. doesn't support saving

GeoTools RDD
^^^^^^^^^^^^

``GeoToolsSpatialRDDProvider`` is provided by the ``geomesa-spark-geotools`` module:

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-spark-geotools_2.11</artifactId>
      // version, etc.
    </dependency>

.. introduction
.. parameters/configuration
.. don't use for Accumulo; use Accumulo provider above instead

Spark SQL
---------

.. introduction
.. custom Spark types (Geometry, Point, Linestring, etc.)
.. how certain queries are pushed down to the Accumulo/GeoTools layer
.. broadcast and joins (and caveats thereof)

Spatial Functions
^^^^^^^^^^^^^^^^^

.. describe functions implemented so far

Usage
-----

.. how to create a new ``SparkSession``/``SparkContext``
.. set up DS and work with them

Jupyter
-------

.. setup: (Toree kernel, etc.)
.. visualization?

Spark 1.6 Support (depreciated)
-------------------------------

.. old docs from previous version of this page (not sure how relevant this
.. is and how much it needs/should be cut down)

To use GeoMesa with Spark 1.6, the executors must know how to serialize and deserialize Simple Features. There are two ways
to accomplish this.

Restart the Spark Context
^^^^^^^^^^^^^^^^^^^^^^^^^

One option is initialize a new Spark Context with the desired data store or SimpleFeatureType.
This involves calling ``GeoMesaSpark.init``, which will take an existing Spark Configuration, and return a new one
that is set to use our GeoMesaSparkKryoRegistrator to serialize Simple Features of the provided types. This return
value must be used to initialize a new Spark Context as ``init`` will also set system properties for all executors
such that they can serialize features of those types.
Multiple data stores are able to be initialized by continually passing the resulting configuration into multiple calls
to ``init`` and restarting the context using the final result.

.. code-block:: scala

    // Call initialize, retrieving a new spark config
    val newConfig = GeoMesaSpark.init(new SparkConf(), dataStore)
    // Stop all other running Spark Contexts
    Option(sc).foreach(_.stop())
    // Initialize a new one with the desired config
    val sc = new SparkContext(newConfig)

Broadcast Simple Feature Types
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Alternatively, if a restart of the Spark Context is undesirable, one is able to register classes directly into the Kryo
Registrator. To do this, take advantage of the fact that basic Spark configuration values can be set in ``spark-defaults.conf``
In this pattern, a call to ``GeoMesaSpark.register`` will register the Simple Feature Types of a provided data store,
skipping the need use shared system properties that require a restart. The caveat, however, is that before serialization,
the SimpleFeatureType encodings must be sent to the executors via a Spark Broadcast and then used to create the corresponding
types in each executor's registrator.

.. code-block:: scala

    // Register the sfts of a given data store
    GeoMesaSpark.register(dataStore)
    // Broadcast sft encodings to executors
    val broadcastedSfts = sc.broadcast(sfts.map{ sft => (sft.getTypeName, SimpleFeatureTypes.encodeType(sft)})
    // Populate the type cache on each partition
    someRdd.foreachPartition { iter =>
        broadcastedSfts.value.foreach { case (name, spec) =>
            val sft = SimpleFeatureTypes.createType(name, spec)
            GeoMesaSparkKryoRegistrator.putType(sft)
        }
    }

Connect to Data Stores
^^^^^^^^^^^^^^^^^^^^^^

GeoMesa Spark further provides functionality to read a data store schema into a Spark RDD. To do this, it is best to
place connection credentials into ``spark-defaults.conf`` in ``${SPARK_HOME}/conf``. With this, set up the connection
parameters.

.. code-block:: scala

    val params = Map(
      "instanceId" -> "mycloud",
      "zookeepers" -> "zoo1,zoo2,zoo3",
      "user"       -> sc.getConf.get("spark.credentials.ds.username"),
      "password"   -> sc.getConf.get("spark.credentials.ds.password"),
      "tableName"  -> "mytable")

And create the RDD

.. code-block:: scala

    val rdd = GeoMesaSpark.rdd(new Configuration, sc, params, query)


Further Examples
----------------

For a complete example of analysis with Spark, see :doc:`../tutorials/spark`

