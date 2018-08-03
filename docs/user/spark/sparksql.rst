SparkSQL
--------

GeoMesa SparkSQL support builds upon the ``DataSet``/``DataFrame`` API present
in the Spark SQL module to provide geospatial capabilities. This includes
custom geospatial data types and functions, the ability to create a ``DataFrame``
from a GeoTools ``DataStore``, and optimizations to improve SQL query performance.

GeoMesa SparkSQL code is provided by the ``geomesa-spark-sql`` module:

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-spark-sql_2.11</artifactId>
      // version, etc.
    </dependency>

Example
^^^^^^^

The following is a Scala example of connecting to GeoMesa Accumulo
via SparkSQL:

.. code-block:: scala

    // DataStore params to a hypothetical GeoMesa Accumulo table
    val dsParams = Map(
      "accumulo.instance.id"   -> "instance",
      "accumulo.zookeepers"    -> "zoo1,zoo2,zoo3",
      "accumulo.user"          -> "user",
      "accumulo.password"      -> "*****",
      "accumulo.catalog"       -> "geomesa_catalog",
      "geomesa.security.auths" -> "USER,ADMIN")

    // Create SparkSession
    val sparkSession = SparkSession.builder()
      .appName("testSpark")
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

Configuration
^^^^^^^^^^^^^

Because GeoMesa SparkSQL stacks on top of the ``geomesa-spark-core`` module,
one or more of the ``SpatialRDDProvider`` implementations must be included on the
classpath. See :ref:`spark_core_config` for details on setting up the Spark classpath.

.. note::

    In most cases, it is not necessary to set up the Kryo serialization described in
    :ref:`spark_sf_serialization` when using SparkSQL. However, this may be required when
    using the :ref:`geotools_rdd_provider`.

If you will be ``JOIN``-ing multiple ``DataFrame``\s together, it will be necessary
to add the ``spark.sql.crossJoin.enabled`` property when creating the
``SparkSession`` object:

.. code-block:: scala

    val spark = SparkSession.builder().
       // ...
       config("spark.sql.crossJoin.enabled", "true").
       // ...
       getOrCreate()

.. warning::

    Cross-joins can be very, very inefficient. Take care to ensure that one or both
    sets of data joined are very small, and consider using the ``broadcast()`` method
    to ensure that at least one ``DataFrame`` joined is in memory.

Usage
^^^^^

To create a GeoMesa SparkSQL-enabled ``DataFrame`` with data corresponding to a particular
feature type, do the following:

.. code-block:: scala

    // dsParams contains the parameters to pass to the data store
    val dataFrame = sparkSession.read
      .format("geomesa")
      .options(dsParams)
      .option("geomesa.feature", typeName)
      .load()

Specifically, invoking ``format("geomesa")`` registers the GeoMesa SparkSQL data source, and
``option("geomesa.feature", typeName)`` tells GeoMesa to use the feature type
named  ``typeName``. This also registers the custom user-defined types and functions
implemented in GeoMesa SparkSQL.

By registering a ``DataFrame`` as a temporary view, it is possible to access
this data frame in subsequent SQL calls. For example:

.. code-block:: scala

    dataFrame.createOrReplaceTempView("chicago")

makes it possible to call this data frame via the alias "chicago":

.. code-block:: scala

    val sqlQuery = "select * from chicago where st_contains(st_makeBBOX(0.0, 0.0, 90.0, 90.0), geom)"
    val resultDataFrame = sparkSession.sql(sqlQuery)

Registering user-defined types and functions can also be done manually by invoking
``SQLTypes.init()`` on the ``SQLContext`` object of the Spark session:

.. code-block:: scala

    SQLTypes.init(sparkSession.sqlContext)

It is also possible to write a Spark DataFrame to a GeoMesa table with:

.. code-block:: scala

    dataFrame.write.format("geomesa").options(dsParams).option("geomesa.feature", "featureName").save()

This will automatically convert the data frame's underlying RDD[Row] into an RDD[SimpleFeature] and write to the
data store in parallel. For this to work, the feature type `featureName` must already exist in the data store.

When writing features back, it is possible to specify the feature ID through the special ``__fid__`` column:

.. code-block:: scala

    dataFrame
        .withColumn("__fid__", $"custom_fid")
        .write
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "featureName")
        .save

Geospatial User-defined Types and Functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The GeoMesa SparkSQL module takes several `classes representing geometry objects`_
(as described by the OGC `OpenGIS Simple feature access common architecture`_ specification and
implemented by the Java Topology Suite) and registers them as user-defined types (UDTs) in
SparkSQL. For example the ``Geometry`` class is registered as ``GeometryUDT``. In GeoMesa SparkSQL
the following types are registered:

 * ``GeometryUDT``
 * ``PointUDT``
 * ``LineStringUDT``
 * ``PolygonUDT``
 * ``MultiPointUDT``
 * ``MultiLineStringUDT``
 * ``MultiPolygonUDT``
 * ``GeometryCollectionUDT``

GeoMesa SparkSQL also implements a subset of the functions described in the
OGC `OpenGIS Simple feature access SQL option`_ specification as SparkSQL
user-defined functions (UDFs). These include functions
for creating geometries, accessing properties of geometries, casting
Geometry objects to more specific subclasses, outputting geometries in other
formats, measuring spatial relationships between geometries, and processing
geometries.

For example, the following SQL query

.. code::

    select * from chicago where st_contains(st_makeBBOX(0.0, 0.0, 90.0, 90.0), geom)

uses two UDFs--``st_contains`` and ``st_makeBBOX``--to find the rows in the ``chicago``
``DataFrame`` where column ``geom`` is contained within the specified bounding box.

A complete list of the implemented UDFs is given in the next section (:doc:`./sparksql_functions`).

.. _classes representing geometry objects: http://docs.geotools.org/stable/userguide/library/jts/geometry.html

.. _OpenGIS Simple feature access common architecture: http://www.opengeospatial.org/standards/sfa

.. _OpenGIS Simple feature access SQL option: http://www.opengeospatial.org/standards/sfs

In-memory Indexing
^^^^^^^^^^^^^^^^^^

If your data is small enough to fit in the memory of your executors, you can tell GeoMesa SparkSQL to persist RDDs in memory
and leverage the use of CQEngine as an in-memory indexed data store. To do this, add the option ``option("cache", "true")``
when creating your data frame. This will place an index on every attribute excluding the ``fid`` and the geometry.
To index based on geometry, add the option ``option("indexGeom", "true")``. Queries to this relation will automatically
hit the cached RDD and query the in-memory data store that lives on each partition, which can yield significant speedups.

Given some knowledge of your data, it is also possible to ensure that the data will fit in memory by applying an initial query.
This can be done with the ``query`` option. For example, ``option("query", "dtg AFTER 2016-12-31T23:59:59Z")``

Spatial Partitioning and Faster Joins
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Additional speedups can be attained by also spatially partitioning your data. Adding the option ``option("spatial", "true")``
will ensure that data that are spatially near each other will be placed on the same partition. By default, your data will
be partitioned into an NxN grid, but there exist 4 total partitioning strategies, and each can be specified by name with
``option("strategy", strategyName)``

  EQUAL - Computes the bounds of your data and divides it into an NxN grid of equal size, where ``N = sqrt(numPartitions)``

  WEIGHTED - Like EQUAL, but ensures that equal proportions of the data along each axis are in each grid cell.

  EARTH - Like EQUAL, but uses the whole earth as the bounds instead of computing them based on the data.

  RTREE - Constructs an R-Tree based on a sample of the data, and uses a subset of the bounding rectangles as partition envelopes.

The advantages to spatially partitioning are two fold:

1) Queries with a spatial predicate that lies wholly in one partition can go directly to that partition, skipping the overhead
of scanning partitions that will be certain to not include the desired data.

2) If two data sets are partitioned by the same scheme, resulting in the same partition envelopes for both relations, then
spatial joins can use the partition envelope as a key in the join. This dramatically reduces the number of comparisons required
to complete the join.

Additional data frame options allow for greater control over how partitions are created. For strategies that require a
sample of the data (WEIGHTED and RTREE), ``sampleSize`` and ``thresholdMultiplier`` can be used to control how much of the
underlying data is used in the decision process and how many items to allow in an RTree envelope.

Other useful options are as follows:

  ``option("partitions", "n")`` - Specifies the number of partitions that the underlying RDDs will be (overrides default parallelism)

  ``option("bounds", "POLYGON in WellKnownText")`` - Limits the bounds of the grid that ``WEIGHTED`` and ``EQUAL`` strategies use.
  All data that do not lie in these bounds will be placed in a separate partition

  ``option("cover", "true")`` - Since only the EQUAL and EARTH partition strategies can guarantee that partition envelopes
  will be identical across relations, data frames with this option set will force the partitioning scheme of data frames
  that they are joined with to match its own.
