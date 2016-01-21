Apache Spark Analysis
=====================

This tutorial will show you how to:

1. Use GeoMesa with `Apache Spark <http://spark.apache.org/>`__.
2. Write custom Scala code for GeoMesa to generate histograms and
   spatial densities of `GDELT <http://www.gdeltproject.org/>`__ event
   data.

Background
----------

`Apache Spark <http://spark.apache.org>`__ is a "fast and general engine
for large-scale data processing". Spark presents an abstraction called a
Resilient Distributed Dataset (RDD) that facilitates expressing
transformations, filters, and aggregations, and efficiently executes the
computation across a distributed set of resources. Spark manages the
lineage of a block of transformed data so that if a node goes down,
Spark can restart the computation for just the missing blocks.

GeoMesa has support for executing Spark jobs over data stored in
GeoMesa. You can initialize a Spark RDD using standard CQL queries and
by passing standard CQL functions to transform the data. In the spirit
of the obligatory Word Count map-reduce example, we demonstrate two
geospatial spins on word count. First, counting features by time
resolution to compute a time series of spatial data, and second,
aggregating by grid cell to rapidly generate density plots. Apache Spark
enables us to express these transformations easily and succinctly.

Prerequisites
-------------

.. raw:: html

   <div class="callout callout-warning">

::

    <span class="glyphicon glyphicon-exclamation-sign"></span>
    You will need access to a Hadoop 2.2 installation as well as an Accumulo |accumulo_version| database.

.. raw:: html

   </div>

.. raw:: html

   <div class="callout callout-warning">

::

    <span class="glyphicon glyphicon-exclamation-sign"></span>
    You will need to have ingested GDELT data using GeoMesa. Instructions are available <a href="/geomesa-gdelt-analysis/">here</a>.

.. raw:: html

   </div>

You will also need:

-  access to a `Spark <http://spark.apache.org/>`__ cluster,
-  an Accumulo user that has appropriate permissions to query your data,
-  Java JDK 7,
-  `Apache Maven <http://maven.apache.org/>`__ 3.2.2 or better, and
-  a `git <http://git-scm.com/>`__ client.

Set Up Tutorial Code
--------------------

Clone the geomesa project and build it, if you haven't already:

.. code-block:: bash

    $ git clone https://github.com/locationtech/geomesa.git
    $ cd geomesa
    $ mvn clean install

This is needed to install the GeoMesa JAR files in your local Maven
repository. For more information see the `GeoMesa Accumulo Quick
Start </geomesa-quickstart/>`__ tutorial.

The code in this tutorial is written in
`Scala <http://scala-lang.org/>`__, as is much of GeoMesa itself. Many
of the commands listed below may be run using the Scala read-eval-print
loop (REPL). The Scala REPL may be invoked in the GeoMesa source
distribution, which will automatically add the GeoMesa JARs to the
classpath, by running:

.. code-block:: bash

    $ mvn scala:console

Count Events by Day of Year
---------------------------

You will need to have ingested some
`GDELT <http://www.gdeltproject.org/>`__ data as described in the `GDELT
Map-Reduce tutorial <http://www.geomesa.org/geomesa-gdelt-analysis/>`__.
First, we get a handle to a GeoMesa data store and construct a CQL query
for our bounding box.

.. code-block:: scala

    val params = Map(
      "instanceId" -> "instance",
      "zookeepers" -> "zoo1,zoo2,zoo3",
      "user"       -> "user",
      "password"   -> "*****",
      "auths"      -> "USER,ADMIN",
      "tableName"  -> "geomesa_catalog")

    val ds = DataStoreFinder.getDataStore(params)

    val ff = CommonFactoryFinder.getFilterFactory2
    val f = ff.bbox("geom", -80, 35, -70, 40, "EPSG:4326")
    val q = new Query("GDELT", f)

Next, initialize an ``RDD[SimpleFeature]`` using ``GeoMesaSpark``.

.. code-block:: scala

    val conf = new Configuration
    val sconf = init(new SparkConf(true), ds)
    val sc = new SparkContext(sconf)

    val queryRDD = geomesa.compute.spark.GeoMesaSpark.rdd(conf, sconf, ds, query)

Finally, we construct our computation which consists of extracting the
``SQLDATE`` from each ``SimpleFeature`` and truncating it to the day
resolution.

.. code-block:: scala

    val dayAndFeature = queryRDD.mapPartitions { iter =>
      val df = new SimpleDateFormat("yyyyMMdd")
      val ff = CommonFactoryFinder.getFilterFactory2
      val exp = ff.property("SQLDATE")
      iter.map { f => (df.format(exp.evaluate(f).asInstanceOf[java.util.Date]), f) }
    }

Then, we group by the day and count up the number of events in each
group.

.. code-block:: scala

    val groupedByDay = dayAndFeature.groupBy { case (date, _) => date }
    val countByDay = groupedByDay.map { case (date, iter) => (date, iter.size) }
    countByDay.collect.foreach(println)

Parallel Computation of Spatial Event Densities
-----------------------------------------------

In the second demonstration, we compute densities of our feature by
discretizing the spatial domain and counting occurrences of the feature
in each grid cell. We use `GeoHashes <http://geohash.org>`__ as our
discretization of the world so that we can configure the resolution of
our density by setting the number of bits in the GeoHash.

First, start with a similar ``RDD[SimpleFeature]`` as before but expand
the bounding box.

.. code-block:: scala

    val f = ff.bbox("geom", -180, -90, 180, 90, "EPSG:4326")
    val q = new Query("GDELT", f)

    val queryRDD = geomesa.compute.spark.GeoMesaSpark.rdd(conf, sconf, ds, query)

Project (in the relational sense) the ``SimpleFeature`` to a 2-tuple of
``(GeoHash, 1)``.

.. code-block:: scala

    val discretized = queryRDD.map { f =>
       (geomesa.utils.geohash.GeoHash(f.getDefaultGeometry.asInstanceOf[Point], 25), 1)
    }

Then, group by grid cell and count the number of features per cell.

.. code-block:: scala

    val density = discretized
       .groupBy { case (gh, _)    => gh }
       .map     { case (gh, iter) => (gh.bbox.envelope, iter.size) }

    density.collect.foreach(println)

The resulting density plot is visualized below.

.. figure:: _static/img/tutorials/2014-08-05-spark/gdelt-global-density.png
   :alt: "Registering new Data Store"

   "Registering new Data Store"
