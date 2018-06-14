.. _avro_converter:

Avro Converter
==============

The Avro converter handles data written by `Apache Avro <http://avro.apache.org/>`__. To use the Avro converter,
specify ``type = "avro"`` in your converter definition.

Configuration
-------------

The Avro converter supports parsing whole Avro files, with the schema embedded, or Avro IPC messages with
the schema omitted. For an embedded schema, set ``schema = "embedded"`` in your converter definition.
For IPC messages, specify the schema in one of two ways: to use an inline schema string, set
``schema = "<schema string>"``; to use a schema defined in a separate file, set ``schema-file = "<path to file>"``.

The Avro record being parsed is available to field transforms as ``$1``.

Avro Paths
----------

Avro paths are defined similarly to JSONPath or XPath, and allow you to extract specific fields out of an
Avro record. An Avro path consists of forward-slash delimited strings. Each part of the path defines
a field name with an optional predicate:

*  ``$type=<typename>`` - match the Avro schema type name on the selected element
*  ``[$<field>=<value>]`` - match elements with a field named "field" and a value equal to "value"

For example, ``/foo$type=bar/baz[$qux=quux]``. See `Example Usage`, below, for a concrete example.

Avro paths are available through the ``avroPath`` transform function, as described below.

.. _avro_converter_functions:

Avro Transform Functions
------------------------

GeoMesa defines several Avro-specific transform functions.

avroPath
^^^^^^^^

Description: Extract values from nested Avro structures.

Usage: ``avroPath($ref, $pathString)``

*  ``$ref`` - a reference object (avro root or extracted object)
*  ``pathString`` - forward-slash delimited path strings. See `Avro Paths`, above

avroBinaryList
^^^^^^^^^^^^^^

GeoMesa has a custom Avro schema for writing SimpleFeatures. List, map and UUID attributes are serialized
as binary Avro fields. This function can read a serialized list-type attribute.

Description: Parses a binary Avro value as a list

Usage: ``avroBinaryList($ref)``

avroBinaryMap
^^^^^^^^^^^^^

GeoMesa has a custom Avro schema for writing SimpleFeatures. List, map and UUID attributes are serialized
as binary Avro fields. This function can read a serialized map-type attribute.

Description: Parses a binary Avro value as a map

Usage: ``avroBinaryMap($ref)``

avroBinaryUuid
^^^^^^^^^^^^^^

GeoMesa has a custom Avro schema for writing SimpleFeatures. List, map and UUID attributes are serialized
as binary Avro fields. This function can read a serialized UUID-type attribute.

Description: Parses a binary Avro value as a UUID

Usage: ``avroBinaryUuid($ref)``

Example Usage
-------------

For this example we'll use the following Avro schema in a file named ``/tmp/schema.avsc``:

::

    {
      "namespace": "org.locationtech",
      "type": "record",
      "name": "CompositeMessage",
      "fields": [
        { "name": "content",
          "type": [
             {
               "name": "DataObj",
               "type": "record",
               "fields": [
                 {
                   "name": "kvmap",
                   "type": {
                      "type": "array",
                      "items": {
                        "name": "kvpair",
                        "type": "record",
                        "fields": [
                          { "name": "k", "type": "string" },
                          { "name": "v", "type": ["string", "double", "int", "null"] }
                        ]
                      }
                   }
                 }
               ]
             },
             {
                "name": "OtherObject",
                "type": "record",
                "fields": [{ "name": "id", "type": "int"}]
             }
          ]
       }
      ]
    }

This schema defines an avro file that has a field named ``content``
which has a nested object which is either of type ``DataObj`` or
``OtherObject``. As an exercise, we can use avro tools to generate some
test data and view it::

    java -jar /tmp/avro-tools-1.7.7.jar random --schema-file /tmp/schema -count 5 /tmp/avro

    $ java -jar /tmp/avro-tools-1.7.7.jar tojson /tmp/avro
    {"content":{"org.locationtech.DataObj":{"kvmap":[{"k":"thhxhumkykubls","v":{"double":0.8793488185997134}},{"k":"mlungpiegrlof","v":{"double":0.45718223406586045}},{"k":"mtslijkjdt","v":null}]}}}
    {"content":{"org.locationtech.OtherObject":{"id":-86025408}}}
    {"content":{"org.locationtech.DataObj":{"kvmap":[]}}}
    {"content":{"org.locationtech.DataObj":{"kvmap":[{"k":"aeqfvfhokutpovl","v":{"string":"kykfkitoqk"}},{"k":"omoeoo","v":{"string":"f"}}]}}}
    {"content":{"org.locationtech.DataObj":{"kvmap":[{"k":"jdfpnxtleoh","v":{"double":0.7748286862915655}},{"k":"bueqwtmesmeesthinscnreqamlwdxprseejpkrrljfhdkijosnogusomvmjkvbljrfjafhrbytrfayxhptfpcropkfjcgs","v":{"int":-1787843080}},{"k":"nmopnvrcjyar","v":null},{"k":"i","v":{"string":"hcslpunas"}}]}}}

Here's a more relevant sample record::

    {
      "content" : {
        "org.locationtech.DataObj" : {
          "kvmap" : [ {
            "k" : "lat",
            "v" : {
              "double" : 45.0
            }
          }, {
            "k" : "lon",
            "v" : {
              "double" : 45.0
            }
          }, {
            "k" : "prop3",
            "v" : {
              "string" : " foo "
            }
          }, {
            "k" : "prop4",
            "v" : {
              "double" : 1.0
            }
          } ]
        }
      }
    }

Let's say we want to convert our Avro array of kvpairs into a simple
feature. We notice that there are 4 attributes:

-  lat
-  lon
-  prop3
-  prop4

The following converter config would be sufficient to parse the Avro::

    {
      type        = "avro"
      schema-file = "/tmp/schema.avsc"
      sft         = "testsft"
      id-field    = "uuid()"
      fields = [
        { name = "tobj", transform = "avroPath($1, '/content$type=DataObj')" },
        { name = "lat",  transform = "avroPath($tobj, '/kvmap[$k=lat]/v')" },
        { name = "lon",  transform = "avroPath($tobj, '/kvmap[$k=lon]/v')" },
        { name = "geom", transform = "point($lon, $lat)" }
      ]
    }
