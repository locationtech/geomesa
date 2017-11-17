JTS Spark
---------

The JTS Spark module provides a set of User Defined Functions (UDFs) and User
Defined Types (UDTs) that enable executing SQL queries in spark that perform
geospatial operations on geospatial data types.
on
GeoMesa SparkSQL support builds upon the ``DataSet``/``DataFrame`` API present
in the Spark SQL module to provide geospatial capabilities. This includes
custom geospatial data types and functions, the ability to create a ``DataFrame``
from a GeoTools ``DataStore``, and optimizations to improve SQL query performance.

This functionality is located in the ``geomesa-jts-spark`` module:

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-jts-spark_2.11</artifactId>
      // version, etc.
    </dependency>

Example
^^^^^^^

The following is a Scala example of loading a DataFrame with user defined types

.. code-block:: scala

    val schema = StructType(Array(StructField("name",StringType, nullable=false),
                                StructField("polygonText", StringType, nullable=false),
                                StructField("latitude", DoubleType, nullable=false),
                                StructField("longitude", DoubleType, nullable=false)))

    val dataFile = this.getClass.getClassLoader.getResource("jts-example.csv").getPath
    df = spark.read.schema(schema).option("sep", "-").csv(dataFile)

    val polyFromText = udf(SQLGeometricConstructorFunctions.ST_PolygonFromText)
    val pointFromCoord = udf(SQLGeometricConstructorFunctions.ST_MakePoint)
    alteredDF = df.withColumn("polygon", polyFromText(col("polygonText")))
                  .withColumn("point", pointFromCoord(col("latitude"), col("longitude")))



Notice how the initial schema does not have a UserDefinedType, but after applying our
User Defined Functions to the appropriate columns, we are left with a data frame with
 geospatial column types.

Configuration
^^^^^^^^^^^^^

To enable this behavior, you must pass the  `SQLContext` of the `SparkSession` to
`SQLTypes.init`. This will register the UDFs and UDTs as well as some catalyst
optimizations for these operations.

.. code-block:: bash

    val spark: SparkSession = SparkSession.builder() // ... initialize spark session
    val sc: SQLContext = spark.sqlContext
    SQLTypes.init(sc)

Geospatial User-defined Types and Functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The JTS Spark module takes several `classes representing geometry objects`_
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

JTS Spark also implements a subset of the functions described in the
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

