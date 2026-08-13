.. _avro_converter:

Avro Converter
==============

The Avro converter handles data written by `Apache Avro <https://avro.apache.org/>`__.

Configuration
-------------

The Avro converter supports the following configuration keys:

================ ======= ==========================================================================================
Key              Type    Description
================ ======= ==========================================================================================
``type``         String  Must be the string ``avro``.
``schema``       String  The Avro schema used for parsing
``schema-file``  String  The path to an Avro schema on the classpath
``schema-files`` List    A list of paths to Avro schemas on the classpath
================ ======= ==========================================================================================

Exactly one of ``schema``, ``schema-file``, or ``schema-files`` must be specified - see below for details.

Avro Formats
------------

The Avro converter supports parsing Avro files in different formats. The schema configuration will determine
what format the converter expects.

Object Container Files
^^^^^^^^^^^^^^^^^^^^^^

For portability, some Avro files contain the schema
`embedded in the file <https://avro.apache.org/docs/1.11.4/specification/#object-container-files>`__. To parse this type of file,
specify ``schema = "embedded"``.

Files with External Schemas
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To parse files or IPC messages that do not have a schema, the schema must be provided externally. To parse this type of file,
specify the schema in one of two ways: as an inline schema string using ``schema = "<schema string>"``; or as a schema file
available on the classpath using ``schema-file = "<path to file>"``.

Single Object Encoding
^^^^^^^^^^^^^^^^^^^^^^

`Single Object Encoding <https://avro.apache.org/docs/1.11.4/specification/#single-object-encoding>`__ is a way to specify
just the fingerprint of a schema as a prefix of the message. The schemas themselves must still be provided externally. To parse
this type of message, specify ``schema-files = [ "<path to file 1>", "<path to file 2>" ]``.

Schema Registry
^^^^^^^^^^^^^^^

To parse messages where schemas are stored in a schema registry, see :ref:`avro_schema_registry_converter`.

.. _avro_converter_functions:

Transform Functions
-------------------

The current Avro record being parsed is available to field transforms as ``$1``. The original message bytes are available
as ``$0``, which may be useful for generating consistent feature IDs.

In addition to the standard :ref:`converter_functions`, the Avro converter provides the following Avro-specific functions:

avroPath
^^^^^^^^

Description: Extract values from nested Avro structures.

Usage: ``avroPath($ref, $pathString)``

*  ``$ref`` - a reference object (avro root or extracted object)
*  ``pathString`` - forward-slash delimited path strings

Avro paths are defined similarly to JSONPath or XPath, and allow you to extract specific fields out of an
Avro record. An Avro path consists of forward-slash delimited strings. Each part of the path defines
a field name with an optional predicate:

*  ``$type=<typename>`` - match the Avro schema type name on the selected element
*  ``[$<field>=<value>]`` - match elements with a field named "field" and a value equal to "value"

For example, ``/foo$type=bar/baz[$qux=quux]``. See the example below for a concrete example.

avroToJson
^^^^^^^^^^

Description: Converts Avro objects to JSON strings.

Usage: ``avroToJson($ref)``

*  ``$ref`` - a reference object (avro root or extracted object)

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

For this example we'll use the following Avro schema in a classpath file named ``schema.avsc``:

::

    {
      "namespace": "org.locationtech",
      "type": "record",
      "name": "CompositeMessage",
      "fields": [
        {
          "name": "content",
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

    java -jar avro-tools-1.11.4.jar random --schema-file schema.avsc -count 5 /tmp/avro

    $ java -jar /tmp/avro-tools-1.11.4.jar tojson /tmp/avro
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
      schema-file = "schema.avsc"
      id-field    = "uuid()"
      fields = [
        { name = "tobj", transform = "avroPath($1, '/content$type=DataObj')" },
        { name = "lat",  transform = "avroPath($tobj, '/kvmap[$k=lat]/v')" },
        { name = "lon",  transform = "avroPath($tobj, '/kvmap[$k=lon]/v')" },
        { name = "geom", transform = "point($lon, $lat)" }
      ]
    }
