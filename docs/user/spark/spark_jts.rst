Spark JTS
---------

The Spark JTS module provides a set of User Defined Functions (UDFs) and User
Defined Types (UDTs) that enable executing SQL queries in spark that perform
geospatial operations on geospatial data types.

GeoMesa SparkSQL support builds upon the ``DataSet``/``DataFrame`` API present
in the Spark SQL module to provide geospatial capabilities. This includes
custom geospatial data types and functions, the ability to create a ``DataFrame``
from a GeoTools ``DataStore``, and optimizations to improve SQL query performance.

This functionality is located in the ``geomesa-spark-jts`` module:

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-spark-jts_2.11</artifactId>
      // version, etc.
    </dependency>

Example
^^^^^^^

The following is a Scala example of loading a DataFrame with user defined types:

.. code-block:: scala

    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.jts.JTSTypes
    import org.locationtech.geomesa.spark.SQLGeometricConstructorFunctions._

    val schema = StructType(Array(StructField("name",StringType, nullable=false),
                                StructField("polygonText", StringType, nullable=false),
                                StructField("latitude", DoubleType, nullable=false),
                                StructField("longitude", DoubleType, nullable=false)))

    val dataFile = this.getClass.getClassLoader.getResource("jts-example.csv").getPath
    df = spark.read.schema(schema).option("sep", "-").csv(dataFile)

    alteredDF = df.withColumn("polygon", st_polyFromText(col("polygonText")))
                  .withColumn("point", st_makePoint(col("latitude"), col("longitude")))



Notice how the initial schema does not have a UserDefinedType, but after applying our
User Defined Functions to the appropriate columns, we are left with a data frame with
geospatial column types.


It is also possible to construct a DataFrame from a list of geospatial objects:

.. code-block:: scala

    import org.locationtech.geomesa.spark.SpatialEncoders._
    val df = spark.createDataset(Seq(point)).toDF()

Configuration
^^^^^^^^^^^^^

To enable this behavior, you must pass the  ``SQLContext`` of the ``SparkSession`` to
``JTSTypes.init``. This will register the UDFs and UDTs as well as some catalyst
optimizations for these operations.

.. code-block:: scala

    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.SQLContext
    import org.apache.spark.sql.jts.JTSTypes

    val spark: SparkSession = SparkSession.builder() // ... initialize spark session
    val sc: SQLContext = spark.sqlContext
    JTSTypes.init(sc)

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

    import org.locationtech.geomesa.spark.SQLSpatialFunctions.st_contains
    import org.locationtech.geomesa.spark.SQLGeometricConstructorFunctions.st_makeBBOX
    import org.apache.spark.sql.functions._
    chicagoDF.where(st_contains(st_makeBBOX(lit(0.0), lit(0.0), lit(90.0), lit(90.0)), col("geom")))

A complete list of the implemented UDFs is given in the next section (:doc:`./sparksql_functions`).

.. _classes representing geometry objects: http://docs.geotools.org/stable/userguide/library/jts/geometry.html

.. _OpenGIS Simple feature access common architecture: http://www.opengeospatial.org/standards/sfa

.. _OpenGIS Simple feature access SQL option: http://www.opengeospatial.org/standards/sfs

Building
^^^^^^^^

This module can be built and used independently of GeoMesa with the following command:

.. code:: bash

    $ mvn install -pl geomesa-spark-jts
