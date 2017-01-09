SparkSQL
--------

GeoMesa SparkSQL support builds upon the ``DataSet``/``DataFrame`` API present
in Spark SQL module to provide geospatial capabilities. This includes
custom geospatial data types and functions, the ability to create a DataFrame
from a GeoTools DataStore, and optimizations to improve SQL query performance.

GeoMesa SparkSQL code is provided by the ``geomesa-spark-sql`` module:

.. code-block:: xml

    <dependency>
      <groupId>org.locationtech.geomesa</groupId>
      <artifactId>geomesa-spark-sql_2.11</artifactId>
      // version, etc.
    </dependency>

Usage
^^^^^

.. code-block::




Geospatial User-defined Types
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The GeoMesa SparkSQL module takes a number fo defines a number of user-defined types in SparkSQL
for working with geospatial data:

 *

Geospatial User-defined

UDFs and UDTs
^^^^^^^^^^^^^

Additionally, UDTs (user defined types) are available for any Geometry type e.g.
Geometry, Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, and GeometryCollection.

//.. code-block:: scala

Query Optimizations
^^^^^^^^^^^^^^^^^^^

.. custom Spark types (Geometry, Point, Linestring, etc.)
.. how certain queries are pushed down to the Accumulo/GeoTools layer
.. broadcast and joins (and caveats thereof)?
