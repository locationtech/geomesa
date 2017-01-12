Spark Core
----------

``geomesa-spark-core`` is used to work directly with ``RDD``\ s of features
from GeoMesa.

Example
^^^^^^^

The following is a complete Scala example of creating an RDD via a geospatial query
against a GeoMesa data store:

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
    val conf = new SparkConf().setMaster("local[*]").setAppName("testSpark")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
    val sc = SparkContext.getOrCreate(conf)

    // create RDD with a geospatial query using GeoMesa functions
    val spatialRDDProvider = GeoMesaSpark(dsParams)
    val filter = ECQL.toFilter("CONTAINS(POLYGON((0 0, 0 90, 90 90, 90 0, 0 0)), geom)")
    val query = new Query("chicago", filter)
    val resultRDD = spatialRDDProvider.rdd(new Configuration, sc, dsParams, query)

    resultRDD.collect
    // Array[org.opengis.feature.simple.SimpleFeature] = Array(
    //    ScalaSimpleFeature:4, ScalaSimpleFeature:5, ScalaSimpleFeature:6,
    //    ScalaSimpleFeature:7, ScalaSimpleFeature:9)

Usage
^^^^^

To use this module, Spark must be configured to register the
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
converter library. ``GeoMesaSpark`` loads a ``SpatialRDDProvider``
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

.. _accumulo_rdd_provider:

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

.. _converter_rdd_provider:

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
 * ``geomesa.sft`` - the ``SimpleFeatureType``, as a spec string, configuration string, or environment lookup name
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

.. _geotools_rdd_provider:

GeoTools RDD Provider
^^^^^^^^^^^^^^^^^^^^^

``GeoToolsSpatialRDDProvider`` is provided by the ``geomesa-spark-geotools`` module:

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-spark-geotools_2.11</artifactId>
      // version, etc.
    </dependency>

``GeoToolsSpatialRDDProvider`` generates and saves ``RDD``\ s of features stored in
a generic GeoTools ``DataStore``. The configuration parameters passed are the same as
those passed to ``DataStoreFinder.getDataStore()`` to create the data store of interest,
plus a required boolean parameter called "geotools" to indicate to the SPI to load
``GeoToolsSpatialRDDProvider``. For example, the `CSVDataStore`_ described in the
`GeoTools ContentDataStore tutorial`_ takes a single parameter called "file". To use
this data store with GeoMesa Spark, do the following:

.. code-block:: scala

    val params = Map(
      "geotools" -> "true",
      "file" -> "locations.csv")
    val query = new Query("locations")
    val rdd = GeoMesaSpark(params).rdd(new Configuration(), sc, params, query)

.. _GeoTools ContentDataStore tutorial: http://docs.geotools.org/latest/userguide/tutorial/datastore/index.html

.. _CSVDataStore: http://docs.geotools.org/latest/userguide/tutorial/datastore/read.html

The name of the feature type to access in the data store is passed as the type name of the
query passed to the ``rdd()`` method. In the example of the `CSVDataStore`_, this is the
basename of the filename passed as an argument.

.. warning::

    Do not use the GeoTools RDD provider with a GeoMesa Accumulo data store. The
    :ref:`accumulo_rdd_provider` provides additional optimizations to improve performance
    between Spark/SparkSQL and GeoMesa Accumulo data stores.

If your data store supports it, use the ``save()`` method to save features:

.. code-block:: scala

    GeoMesaSpark(params).save(rdd, params, "locations")
