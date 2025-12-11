.. _parquet_converter:

Parquet Converter
=================

The Parquet converter handles data written by `Apache Parque <https://parquet.apache.org/>`__.

The Parquet converter supports parsing whole Parquet files. Due to the Parquet random-access API, it is important to set the
input file path in the evaluation context. This is handled automatically by the GeoMesa CLI tools, but if used programmatically
``"inputFilePath"`` must be set in the evaluation context global parameters.

Configuration
-------------

The Parquet converter supports the following configuration keys:

=============== ======== ======= ==========================================================================================
Key             Required Type    Description
=============== ======== ======= ==========================================================================================
``type``        yes      String  Must be the string ``parquet``.
=============== ======== ======= ==========================================================================================

Transform Functions
-------------------

As Parquet does not define any object model, standard practice is to parse Parquet files into Avro GenericRecords. The current
Avro GenericRecord being parsed is available to field transforms as ``$0``.

Because Parquet files are converted into Avro records, it is possible to use Avro paths to select elements. See
:ref:`avro_converter` for details on Avro paths. Note that the result of an Avro path expression will be typed
appropriately according to the Parquet column type (e.g. String, Double, List, etc).

In addition to the standard :ref:`converter_functions`, the Parquet converter provides the following Parquet-specific functions:

parquetPoint
^^^^^^^^^^^^

Description: Parses a nested Point structure from a Parquet record

Usage: ``parquetPoint($ref)``

*  ``$ref`` - a reference object (Avro root record or extracted object)

The point function can parse GeoMesa-encoded Point columns, which consist of a Parquet group of two double-type
columns named ``x`` and ``y``.

parquetLineString
^^^^^^^^^^^^^^^^^

Description: Parses a nested LineString structure from a Parquet record

Usage: ``parquetLineString($ref)``

*  ``$ref`` - a reference object (Avro root record or extracted object)

The linestring function can parse GeoMesa-encoded LineString columns, which consist of a Parquet group of two
repeated double-type columns named ``x`` and ``y``.

parquetPolygon
^^^^^^^^^^^^^^

Description: Parses a nested Polygon structure from a Parquet record

Usage: ``parquetPolygon($ref)``

*  ``$ref`` - a reference object (Avro root record or extracted object)

The polygon function can parse GeoMesa-encoded Polygon columns, which consist of a Parquet group of two list-type
columns named ``x`` and ``y``. The list elements are repeated double-type columns.

parquetMultiPoint
^^^^^^^^^^^^^^^^^

Description: Parses a nested MultiPoint structure from a Parquet record

Usage: ``parquetMultiPoint($ref)``

*  ``$ref`` - a reference object (Avro root record or extracted object)

The multi-point function can parse GeoMesa-encoded MultiPoint columns, which consist of a Parquet group of two
repeated double-type columns named ``x`` and ``y``.

parquetMultiLineString
^^^^^^^^^^^^^^^^^^^^^^

Description: Parses a nested MultiLineString structure from a Parquet record

Usage: ``parquetMultiLineString($ref)``

*  ``$ref`` - a reference object (Avro root record or extracted object)

The multi-linestring function can parse GeoMesa-encoded MultiLineString columns, which consist of a Parquet group
of two list-type columns named ``x`` and ``y``. The list elements are repeated double-type columns.

parquetMultiPolygon
^^^^^^^^^^^^^^^^^^^

Description: Parses a nested MultiPolygon structure from a Parquet record

Usage: ``parquetMultiPolygon($ref)``

*  ``$ref`` - a reference object (Avro root record or extracted object)

The multi-polygon function can parse GeoMesa-encoded MultiPolygon columns, which consist of a Parquet group
of two list-type columns named ``x`` and ``y``. The list elements are also lists, and the nested list elements
are repeated double-type columns.

Example Usage
-------------

For this example we'll consider the following JSON file:

.. code-block:: json

  { "id": 1, "number": 123, "color": "red",   "physical": { "weight": 127.5,   "height": "5'11" }, "lat": 0,   "lon": 0 }
  { "id": 2, "number": 456, "color": "blue",  "physical": { "weight": 150,     "height": "5'11" }, "lat": 1,   "lon": 1 }
  { "id": 3, "number": 789, "color": "green", "physical": { "weight": 200.4,   "height": "6'2" },  "lat": 4.4, "lon": 3.3 }

This file can be converted to Parquet using Spark:

.. code-block:: scala

  import org.apache.spark.sql.SparkSession
  val session = SparkSession.builder().appName("testSpark").master("local[*]").getOrCreate()
  val df = session.read.json("/tmp/example.json")
  df.write.option("compression","gzip").parquet("/tmp/example.parquet")

The following SimpleFeatureType and converter would be sufficient to parse the resulting Parquet file:

.. code-block:: json

  {
    "geomesa" : {
      "sfts" : {
        "example" : {
           "fields" : [
            { "name" : "color",  "type" : "String" }
            { "name" : "number", "type" : "Long"   }
            { "name" : "height", "type" : "String" }
            { "name" : "weight", "type" : "Double" }
            { "name" : "geom",   "type" : "Point", "srid" : 4326 }
          ]
        }
      },
      "converters" : {
        "example" : {
          "type" : "parquet",
          "id-field" : "avroPath($0, '/id')",
          "fields" : [
            { "name" : "color",  "transform" : "avroPath($0,'/color')" },
            { "name" : "number", "transform" : "avroPath($0,'/number')" },
            { "name" : "height", "transform" : "avroPath($0,'/physical/height')" },
            { "name" : "weight", "transform" : "avroPath($0,'/physical/weight')" },
            { "name" : "geom",   "transform" : "point(avroPath($0,'/lon'),avroPath($0,'/lat'))" }
          ],
          "options" : {
            "encoding" : "UTF-8",
            "error-mode" : "log-errors",
            "parse-mode" : "incremental",
            "validators" : [ "index" ]
          }
        }
      }
    }
  }
