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

.. warning::

    You will need access to a Hadoop |hadoop_version| installation with Yarn as well as an Accumulo |accumulo_version| database.

    You will need to have ingested GDELT data using GeoMesa. Instructions are available in :doc:`geomesa-examples-gdelt`.

You will also need:

-  a `Spark <http://spark.apache.org/>`__ 1.5.0 or later distribution (see below)
-  an Accumulo user that has appropriate permissions to query your data
-  Java JDK 7
-  `Apache Maven <http://maven.apache.org/>`__ 3.2.2 or better
-  a `git <http://git-scm.com/>`__ client

Spark Distribution
------------------

Spark does not provide a Scala 2.11 (required for GeoMesa) distribution directly. Instead, you have to
build Spark from source. You can follow the instructions :here:`http://spark.apache.org/docs/latest/building-spark.html`,
with Scala 2.11 details :here:`http://spark.apache.org/docs/latest/building-spark.html#building-for-scala-211`.

.. warning::

    Ensure that you are using Spark 1.5.0 or later

GeoMesa works best with Spark running on Yarn - as such, you need to have an available Hadoop with Yarn
installation alongside your Spark distribution. We will use ``spark-submit`` to run our jobs on the cluster.

Set Up Tutorial Code
--------------------

Clone the geomesa project and build it, if you haven't already:

.. code-block:: bash

    $ git clone https://github.com/locationtech/geomesa.git
    $ cd geomesa
    $ mvn clean install

This is needed to install the GeoMesa JAR files in your local Maven
repository. For more information see the :doc:`../developer/index`.

The code in this tutorial is written in
`Scala <http://scala-lang.org/>`__, as is much of GeoMesa itself.

Count Events by Day of Year
---------------------------

You will need to have ingested some
`GDELT <http://www.gdeltproject.org/>`__ data as described in :doc:`geomesa-examples-gdelt`.
We have an example analysis in the class
``geomesa-compute/src/main/scala/org/locationtech/geomesa/compute/spark/analytics/CountByDay.scala``.

The code goes as follows.

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
    val q = new Query("event", ECQL.toFilter(filter))

Next, initialize an ``RDD[SimpleFeature]`` using ``GeoMesaSpark``.

.. code-block:: scala

    val sc = new SparkContext(GeoMesaSpark.init(new SparkConf(true), ds))
    val queryRDD = GeoMesaSpark.rdd(new Configuration, sc, params, q, None)

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
    countByDay.collect().foreach(println)

Run the Tutorial Code
^^^^^^^^^^^^^^^^^^^^^

Edit the file ``geomesa-compute/src/main/scala/org/locationtech/geomesa/compute/spark/analytics/CountByDay.scala``
so that the parameter map points to your cloud instance. Ensure that the ``filter`` covers
a valid range of your GDELT data.

Re-build the GeoMesa Spark jar to pick up the changes:

.. code-block:: bash

    $ mvn clean install -pl geomesa-compute

Now, we can submit the job to our Yarn cluster using ``spark-submit``:

.. code-block:: bash

    $ /path/to/spark/bin/spark-submit --master yarn-client --num-executors 40 --executor-cores 4 \
        --deploy-mode client --class org.locationtech.geomesa.compute.spark.analytics.CountByDay \
        geomesa-compute/target/geomesa-compute-<version>-shaded.jar

You should see a lot of Spark logging, and then the counts:

.. code-block:: bash

    (20140126,3)
    (20140127,33)
    (20140128,34)
    ...

Parallel Computation of Spatial Event Densities
-----------------------------------------------

In the second demonstration, we compute densities of our feature by
discretizing the spatial domain and counting occurrences of the feature
in each grid cell. We use `GeoHashes <http://geohash.org>`__ as our
discretization of the world so that we can configure the resolution of
our density by setting the number of bits in the GeoHash.

This code is does not exist in GeoMesa; it's left as an exercise for the reader.

First, start with a similar ``RDD[SimpleFeature]`` as before but expand
the bounding box.

.. code-block:: scala

    val f = ff.bbox("geom", -180, -90, 180, 90, "EPSG:4326")
    val q = new Query("event", f)

    val queryRDD = GeoMesaSpark.rdd(new Configuration, sc, params, q, None)

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
