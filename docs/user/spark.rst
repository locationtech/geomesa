GeoMesa Spark
=============

GeoMesa Spark allows for execution of jobs on Apache Spark using data stored in GeoMesa,
other GeoTools ``DataStore``\ s, or files readable by the GeoMesa converter library.
The library allows creation of Spark ``RDD``\ s and ``DataFrame``\ s, writing of
Spark ``RDD``\ s and ``DataFrame``\ s to GeoMesa Accumulo and other GeoTools ``DataStore``\ s, and serialization of ``SimpleFeature``\ s using Kryo.

The current version of GeoMesa Spark supports Apache Spark 2.0.

Examples
--------

Creating an RDD.

.. code-block:: scala

    // DataStore params to a hypothetical GeoMesa Accumulo table
    val dsParams = Map(
      "instanceId" -> "instance",
      "zookeepers" -> "zoo1,zoo2,zoo3",
      "user"       -> "user",
      "password"   -> "*****",
      "auths"      -> "USER,ADMIN",
      "tableName"  -> "geomesa_catalog")

    // set SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("testSpark")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
    val sc = SparkContext.getOrCreate(conf)

    // create RDD with a geospatial query using GeoMesa functions
    val spatialRDDProvider = GeoMesaSpark(dsParams)
    val filter = ECQL.toFilter("CONTAINS(POLYGON((0 0, 0 90, 90 90, 90 0, 0 0)), geom)")
    val query = new Query("chicago", filter)
    val resultRDD = spatialRDDProvider.rdd(new Configuration, sc, dsParams, query)

    resultRDD.collect
    // Array[org.opengis.feature.simple.SimpleFeature] = Array(ScalaSimpleFeature:4, ScalaSimpleFeature:5, ScalaSimpleFeature:6, ScalaSimpleFeature:7, ScalaSimpleFeature:9)

Creating a DataFrame.

.. code-block:: scala

    // DataStore params to a hypothetical GeoMesa Accumulo table
    val dsParams = Map(
      "instanceId" -> "instance",
      "zookeepers" -> "zoo1,zoo2,zoo3",
      "user"       -> "user",
      "password"   -> "*****",
      "auths"      -> "USER,ADMIN",
      "tableName"  -> "geomesa_catalog")

    // Create SparkSession
    val sparkSession = SparkSession.builder()
      .appName("testSpark")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
      .config("spark.sql.crossJoin.enabled", "true")
      .master("local[*]")
      .getOrCreate()

    // Create DataFrame using the "geomesa" format
    val dataFrame = sparkSession.read
      .format("geomesa")
      .options(dsParams)
      .option("geomesa.feature", "chicago")
      .load()
    dataFrame.createOrReplaceTempView("chicago")

    // Query against the "chicago" schema
    val sqlQuery = "select * from chicago where st_contains(st_makeBBOX(0.0, 0.0, 90.0, 90.0), geom)"
    val resultDataFrame = sparkSession.sql(sqlQuery)

    resultDataFrame.show
    /*
    +-------+------+-----------+--------------------+-----------------+
    |__fid__|arrest|case_number|                 dtg|             geom|
    +-------+------+-----------+--------------------+-----------------+
    |      4|  true|          4|2016-01-04 00:00:...|POINT (76.5 38.5)|
    |      5|  true|          5|2016-01-05 00:00:...|    POINT (77 38)|
    |      6|  true|          6|2016-01-06 00:00:...|    POINT (78 39)|
    |      7|  true|          7|2016-01-07 00:00:...|    POINT (20 20)|
    |      9|  true|          9|2016-01-09 00:00:...|    POINT (50 50)|
    +-------+------+-----------+--------------------+-----------------+
    */

Architecture
------------

The ``geomesa-spark-core`` module provides the core of GeoMesa's Spark support,
through the ``GeoMesaSpark`` object...

.. spark-sql
.. include architecture diagram(s) from the Jupyter powerpoint?

Spark Core
----------

``geomesa-spark-core`` is used to work directly with ``RDD``\ s of features
from GeoMesa. To use this module, Spark must be configured to register the
``GeoMesaSparkKryoRegistor`` class, which provides objects to serialize and
deserialize features for each feature type, as shown in the Scala code below:

.. code-block:: scala

    val conf = new SparkConf().setMaster("local[*]").setAppName("testSpark")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
    val sc = SparkContext.getOrCreate(conf)

``geomesa-spark-core`` also provides an API for accessing geospatial data
in Spark, by defining an interface called ``SpatialRDDProvider``. Different
implementations of this interface connect to GeoMesa Accumulo, generic
GeoTools-based ``DataStore``\ s, or data files in formats readable by the GeoMesa
converter library. ``GeoMesaSpark`` loads an ``SpatialRDDProvider``
implementation via SPI when the appropriate JAR is included on the classpath.
The implementation returned by ``GeoMesaSpark`` is chosen based on the
parameters passed as an argument, as shown in the Scala code below:

.. code-block:: scala

    // parameters to pass to the SpatialRDDProvider implementation
    val params = Map(
      "param1" -> "foo",
      "param2" -> "bar")
    // GeoTools Query; may be used to filter results retrieved from the data store
    val query = new Query("foo")
    // val query = new Query("foo", ECQL.toFilter("name like 'A%'"))
    // get the RDD, using the SparkContext configured as above
    val rdd = GeoMesaSpark(params).rdd(new Configuration(), sc, params, query)

Accumulo RDD Provider
^^^^^^^^^^^^^^^^^^^^^

``AccumuloSpatialRDDProvider`` is provided by the ``geomesa-accumulo-spark`` module:

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-accumulo-spark_2.11</artifactId>
      // version, etc.
    </dependency>

This provider generates and saves ``RDD``\ s of features stored in a GeoMesa
``AccumuloDataStore``. The configuration parameters passed to
``AccumuloSpatialRDDProvider`` are the same as those passed to
``AccumuloDataStoreFactory.createDataStore()`` or ``DataStoreFinder.getDataStore()``.
The feature to access in GeoMesa is passed as the type name of the query passed
to the ``rdd()`` method. For example, to load an ``RDD`` of features of type "gdelt"
from the "geomesa" Accumulo table:

.. code-block:: scala

    val params = Map(
      "instanceId" -> "mycloud",
      "user" -> "user",
      "password" -> "password",
      "zookeepers" -> "zoo1,zoo2,zoo3",
      "tableName" -> "geomesa")
    val query = new Query("gdelt")
    val rdd = GeoMesaSpark(params).rdd(new Configuration(), sc, params, query)

To save features, use the ``save()`` method:

.. code-block:: scala

    GeoMesaSpark(params).save(rdd, params, "gdelt")

Converter RDD Provider
^^^^^^^^^^^^^^^^^^^^^^

``ConverterSpatialRDDProvider`` is provided by the ``geomesa-spark-converter`` module:

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-spark-converter_2.11</artifactId>
      // version, etc.
    </dependency>

``ConverterSpatialRDDProvider`` reads features from one or more data files in formats
readable by the :doc:`/user/convert` library, including delimited and fixed-width text,
Avro, JSON, and XML files. It takes the following configuration parameters:

 * ``geomesa.converter`` - the converter defintion as a Typesafe Config string
 * ``geomesa.converter.inputs`` - input file paths, comma-delimited
 * ``geomesa.sft`` - the ``SimpleFeaturetype``, as a spec string, configuration string, or environment lookup name
 * ``geomesa.sft.name`` - (optional) the name of the ``SimpleFeatureType``

Consider the example data described in the :ref:`convert_example_usage` section of the
:doc:`/user/convert` documentation. If the file ``example.csv`` contains the
example data, and ``example.conf`` contains the Typesafe configuration file for the
converter, the following Scala code can be used to load this data into an ``RDD``:

.. code-block:: scala

    val exampleConf = ConfigFactory.load("example.conf").root().render()
    val params = Map(
      "geomesa.converter" -> exampleConf,
      "geomesa.converter.inputs" -> "example.csv",
      "geomesa.sft" -> "phrase:String,dtg:Date,geom:Point:srid=4326",
      "geomesa.sft.name" -> "example")
    val query = new Query("example")
    val rdd = GeoMesaSpark(params).rdd(new Configuration(), sc, params, query)

.. warning::

    ``ConvertSpatialRDDProvider`` is read-only, and does not support writing features
    to data files.

GeoTools RDD Provider
^^^^^^^^^^^^^^^^^^^^^

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

Our GeoMesa Spark SQL support builds upon the DataSet/DataFrame API present
in Spark SQL module to provide geospatial capabilities. This includes adding
custom geospatial data types and functions, the ability to create a DataFrame from a GeoTools DataStore,
and optimizations to improve query performance.
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

