Spark Core
----------

``geomesa-spark-core`` is used to work directly with ``RDD``\ s of features
from GeoMesa and other geospatial data stores.

Example
^^^^^^^

The following is a complete Scala example of creating an RDD via a geospatial query
against a GeoMesa data store:

.. code-block:: scala

    // DataStore params to a hypothetical GeoMesa Accumulo table
    val dsParams = Map(
      "accumulo.instance.id"   -> "instance",
      "accumulo.zookeepers"    -> "zoo1,zoo2,zoo3",
      "accumulo.user"          -> "user",
      "accumulo.password"      -> "*****",
      "accumulo.catalog"       -> "geomesa_catalog",
      "geomesa.security.auths" -> "USER,ADMIN")

    // set SparkContext
    val conf = new SparkConf().setMaster("local[*]").setAppName("testSpark")
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

.. _spark_core_config:

Configuration
^^^^^^^^^^^^^

``geomesa-spark-core`` provides an API for accessing geospatial data
in Spark, by defining an interface called ``SpatialRDDProvider``. Different
implementations of this interface connect to different input sources. These different
providers are described in more detail in :ref:`spark_core_usage` below.

GeoMesa provides several JAR-with-dependencies to simplify setting up the Spark
classpath. To use these libraries in Spark, the appropriate shaded JAR can be passed (for example)
to the ``spark-submit`` command via the ``--jars`` option:

.. code-block:: bash

    --jars file://path/to/geomesa-accumulo-spark-runtime_2.11-$VERSION.jar

or passed to Spark via the appropriate mechanism in notebook servers such as
Jupyter (see :doc:`jupyter`) or Zeppelin.

The shaded JAR should also provide the dependencies needed for the
:ref:`converter_rdd_provider` and :ref:`geotools_rdd_provider`, so these JARs
may simply be added to ``--jars`` as well (though in the latter
case additional JARs may be needed to implement the GeoTools data store accessed).

.. _spark_sf_serialization:

Simple Feature Serialization
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To serialize ``RDD``\ s of ``SimpleFeature``\ s between nodes of a cluster, Spark
must be configured with a Kryo serialization registrator provided in ``geomesa-spark-core``.

.. note::

    Configuring Kryo serialization is not needed when running Spark in ``local``
    mode, as jobs will be executed within a single JVM.

Add these two entries to ``$SPARK_HOME/conf/spark-defaults.conf``
(or pass them as ``--conf`` arguments to ``spark-submit``):

.. code::

    spark.serializer        org.apache.spark.serializer.KryoSerializer
    spark.kryo.registrator  org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator

.. note::

    Alternatively, these may be set in the ``SparkConf`` object used to create the
    ``SparkContext``:

    .. code-block:: scala

        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)

    When using Spark in a notebook server, this will require disabling the automatic
    creation of a ``SparkContext``.

After setting the configuration options, RDDs created by the GeoMesa
``SpatialRDDProvider`` implementations will be properly registered with the
serializer provider.

.. _spark_core_usage:

Usage
^^^^^

The main point of entry for the functionality provided by ``geomesa-spark-core`` is the
``GeoMesaSpark`` object:

.. code-block:: scala

    val spatialRDDProvider = GeoMesaSpark(params)

``GeoMesaSpark`` loads a ``SpatialRDDProvider``
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


To save features, use the ``save()`` method:

.. code-block:: scala

    GeoMesaSpark(params).save(rdd, params, "gdelt")

Note that some providers may be read-only.

See :doc:`./providers` for details on specific provider implementations.
