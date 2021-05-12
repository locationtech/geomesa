Spark JTS
---------

The Spark JTS module provides a set of User Defined Functions (UDFs) and User
Defined Types (UDTs) that enable executing SQL queries in spark that perform
geospatial operations on geospatial data types.

GeoMesa SparkSQL support builds upon the ``DataSet``/``DataFrame`` API present
in the Spark SQL module to provide geospatial capabilities. This includes
custom geospatial data types and functions, the ability to create a ``DataFrame``
from a GeoTools ``DataStore``, and optimizations to improve SQL query performance.

This functionality is located in the ``geomesa-spark/geomesa-spark-jts`` module:

.. parsed-literal::

    <properties>
      <geomesa.version>\ |release_version|\ </geomesa.version>
      <scala.binary.version>\ |scala_binary_version|\ </scala.binary.version>
    </properties>

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
<<<<<<< HEAD
      <artifactId>geomesa-spark-jts_${scala.binary.version}</artifactId>
      <version>${geomesa.version}</version>
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 96d5d442fa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0104fb37e4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7fbbd56493 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
      <artifactId>geomesa-spark-jts_2.12</artifactId>
      // version, etc.
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
      <artifactId>geomesa-spark-jts_2.12</artifactId>
      // version, etc.
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
=======
      <artifactId>geomesa-spark-jts_2.12</artifactId>
      // version, etc.
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 59a1fbb96e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
      <artifactId>geomesa-spark-jts_2.12</artifactId>
      // version, etc.
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 96d5d442fa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0104fb37e4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
      <artifactId>geomesa-spark-jts_2.12</artifactId>
      // version, etc.
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 3d5144418e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d498bef1ce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
      <artifactId>geomesa-spark-jts_2.12</artifactId>
      // version, etc.
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7fbbd56493 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    </dependency>

Example
^^^^^^^

The following is a Scala example of loading a DataFrame with user defined types:

.. code-block:: scala

    import org.locationtech.jts.geom._
    import org.apache.spark.sql.types._
    import org.locationtech.geomesa.spark.jts._

    import spark.implicits._

    val schema = StructType(Array(
      StructField("name",StringType, nullable=false),
      StructField("pointText", StringType, nullable=false),
      StructField("polygonText", StringType, nullable=false),
      StructField("latitude", DoubleType, nullable=false),
      StructField("longitude", DoubleType, nullable=false)))

    val dataFile = this.getClass.getClassLoader.getResource("jts-example.csv").getPath
    val df = spark.read
      .schema(schema)
      .option("sep", "-")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .csv(dataFile)

    val alteredDF = df
      .withColumn("polygon", st_polygonFromText($"polygonText"))
      .withColumn("point", st_makePoint($"longitude", $"latitude"))


Notice how the initial schema does not have a UserDefinedType, but after applying our
User Defined Functions to the appropriate columns, we are left with a data frame with
geospatial column types.

It is also possible to construct a DataFrame from a list of geospatial objects:

.. code-block:: scala

    import spark.implicits._
    val point = new GeometryFactory().createPoint(new Coordinate(3.4, 5.6))
    val df = Seq(point).toDF("point")

Configuration
^^^^^^^^^^^^^

To enable this behavior, import ``org.locationtech.geomesa.spark.jts._``, create a
``SparkSession` and call ``.withJTS`` on it. This will register the UDFs and UDTs as
well as some catalyst optimizations for these operations. Alternatively you can call
``initJTS(SQLContext)``.

.. code-block:: scala

    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.SQLContext
    import org.locationtech.geomesa.spark.jts._

    val spark: SparkSession = SparkSession.builder() // ... initialize spark session
    spark.withJTS


Geospatial User-defined Types and Functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Spark JTS module takes several `classes representing geometry objects`_
(as described by the OGC `OpenGIS Simple feature access common architecture`_ specification and
implemented by the Java Topology Suite) and registers them as user-defined types (UDTs) in
SparkSQL. For example the ``Geometry`` class is registered as ``GeometryUDT``. The following types are registered:

 * ``GeometryUDT``
 * ``PointUDT``
 * ``LineStringUDT``
 * ``PolygonUDT``
 * ``MultiPointUDT``
 * ``MultiLineStringUDT``
 * ``MultiPolygonUDT``
 * ``GeometryCollectionUDT``

Spark JTS also implements a subset of the functions described in the
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

The UDFs are also exposed for use with the DataFrame API, meaning the above example is
also achievable with the following code:

.. code::

    import org.locationtech.geomesa.spark.jts._
    import spark.implicits. _
    chicagoDF.where(st_contains(st_makeBBOX(0.0, 0.0, 90.0, 90.0), $"geom"))

GeoTools User-defined Functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Note that there are three GeoTools derived UDFs and those are:

 * ``st_distanceSpheroid``
 * ``st_lengthSpheroid``
 * ``st_transform``

These are available in the geomesa-spark-sql jar, but also bundled by default in the spark-runtime.
Example usage is as follows:

.. code::

    import org.locationtech.geomesa.spark.geotools._
    chicagoDF.where(st_distanceSpheroid(st_point(0.0,0.0), col("geom")) > 10)


A complete list of the implemented UDFs is given in the next section (:doc:`./sparksql_functions`).

.. _classes representing geometry objects: https://docs.geotools.org/stable/userguide/library/jts/geometry.html

.. _OpenGIS Simple feature access common architecture: https://www.opengeospatial.org/standards/sfa

.. _OpenGIS Simple feature access SQL option: https://www.opengeospatial.org/standards/sfs

.. code::

    import org.locationtech.geomesa.spark.jts._
    import spark.implicits. _
    chicagoDF.where(st_contains(st_makeBBOX(0.0, 0.0, 90.0, 90.0), $"geom"))


Building
^^^^^^^^

This module can be built and used independently of GeoMesa with the following command:

.. code:: bash

    $ mvn install -pl geomesa-spark/geomesa-spark-jts
