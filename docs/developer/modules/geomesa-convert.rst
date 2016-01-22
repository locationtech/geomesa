.. _geomesa-convert:

geomesa-convert
===============

A configurable and extensible library for converting data into
SimpleFeatures

Overview
--------

Converters for various different data formats (currently supported:
delimited text, Avro, fixed width) can be configured and instantiated
using the ``SimpleFeatureConverters`` factory and a target
``SimpleFeatureType``. The converter allows the specification of fields
extracted from the data and transformations on those fields. Syntax of
transformations is very much like ``awk`` syntax. Fields with names that
correspond to attribute names in the ``SimpleFeatureType`` will be
directly populated in the result SimpleFeature. Fields that do not align
with attributes in the ``SimpleFeatureType`` are assumed to be
intermediate fields used for deriving attributes. Fields can reference
other fields by name for building up complex attributes.

Example usage
-------------

Suppose you have a ``SimpleFeatureType`` with the following schema:
``phrase:String,dtg:Date,geom:Point:srid=4326`` and comma-separated data
as shown below.

::

    first,hello,2015-01-01T00:00:00.000Z,45.0,45.0
    second,world,2015-01-01T00:00:00.000Z,45.0,45.0                                                                                                                                                                    

The first two fields should be concatenated together to form the phrase,
the third field should be parsed as a date, and the last two fields
should be formed into a ``Point`` geometry. The following configuration
file defines an appropriate converter for taking this csv data and
transforming it into our ``SimpleFeatureType``.

::

     converter = { 
      type         = "delimited-text",
      format       = "DEFAULT",
      id-field     = "md5($0)",
      fields = [
        { name = "phrase", transform = "concat($1, $2)" },
        { name = "lat",    transform = "$4::double" },
        { name = "lon",    transform = "$5::double" },
        { name = "dtg",    transform = "dateHourMinuteSecondMillis($3)" },
        { name = "geom",   transform = "point($lat, $lon)" }
      ]
     }

The ``id`` of the ``SimpleFeature`` is formed from an md5 hash of the
entire record (``$0`` is the original data) and the other fields are
formed from appropriate transforms.

Transformation functions
------------------------

Currently supported transformation functions are listed below.

String functions
^^^^^^^^^^^^^^^^

-  ``stripQuotes``
-  ``trim``
-  ``capitalize``
-  ``lowercase``
-  ``regexReplace``
-  ``concat``
-  ``substr``
-  ``strlen``

Date functions
^^^^^^^^^^^^^^

-  ``now``
-  ``date``
-  ``isodate``
-  ``isodatetime``
-  ``dateHourMinuteSecondMillis``

Geometry functions
^^^^^^^^^^^^^^^^^^

Geometry functions can transform WKT strings, lat/lon pairs, and GeoJSON
geometry objects:

::

    # { name = "lat", json-type="double", path="$.lat" }
    # { name = "lon", json-type="double", path="$.lon" }
    # { name = "geom", transform="point($lat, $lon)" }
    #
    {
        "lat": 23.9,
        "lon": 24.2,
    }
        
    # { name = "geom", transform="point($2, $3)"
    id,lat,lon,date
    identity1,23.9,24.2,2015-02-03
        
    # { name = "geom", transform="geometry($2)" }
    ID,wkt,date
    1,POINT(2 3),2015-01-02
        
    # { name = "geom", json-type = "geometry", path = "$.geometry" }
    {
        id: 1,
        number: 123,
        color: "red",
        "geometry": {"type": "Point", "coordinates": [55, 56]}
     },

Available transforms are:

-  ``point``
-  ``linestring``
-  ``polygon``
-  ``geometry``

Avro Path functions
^^^^^^^^^^^^^^^^^^^

-  ``avroPath``

Extending the converter library
-------------------------------

There are two ways to extend the converter library - adding new
transformation functions and adding new data formats.

Adding new transformation functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To add new transformation functions, create a
``TransformationFunctionFactory`` and register it in
``META-INF/services/org.locationtech.geomesa.convert.TransformationFunctionFactory``.
For example, here's how to add a new transformation function that
computes a SHA-256 hash.

.. code:: scala

    class SHAFunctionFactory extends TransformerFunctionFactory {
      override def functions = Seq(sha256)
      val sha256fn = TransformerFn("sha256") { args => Hashing.sha256().hashBytes(args(0).asInstanceOf[Array[Byte]]) }
    }

The ``sha256`` function can then be used in a field as shown.

::
       
    ...
    fields: [
       { name = "hash", transform = "sha256($0)" }
    ]

Adding new data formats
^^^^^^^^^^^^^^^^^^^^^^^

To add new data formats, implement the ``SimpleFeatureConverterFactory``
and ``SimpleFeatureConverter`` interfaces and register them in
``META-INF/services`` appropriately. See
``org.locationtech.geomesa.convert.avro.Avro2SimpleFeatureConverter``
for an example.
