.. _avro_converter:

Avro Converter
--------------

The `Avro <http://avro.apache.org/>`_ parsing library is similar to the JSON parsing library. For
this example we'll use the following Avro schema in a file named
``/tmp/schema.avsc``:

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
``OtherObject``. As an exercise...using avro tools we can generate some
test data and view it:

::

    java -jar /tmp/avro-tools-1.7.7.jar random --schema-file /tmp/schema -count 5 /tmp/avro

    $ java -jar /tmp/avro-tools-1.7.7.jar tojson /tmp/avro
    {"content":{"org.locationtech.DataObj":{"kvmap":[{"k":"thhxhumkykubls","v":{"double":0.8793488185997134}},{"k":"mlungpiegrlof","v":{"double":0.45718223406586045}},{"k":"mtslijkjdt","v":null}]}}}
    {"content":{"org.locationtech.OtherObject":{"id":-86025408}}}
    {"content":{"org.locationtech.DataObj":{"kvmap":[]}}}
    {"content":{"org.locationtech.DataObj":{"kvmap":[{"k":"aeqfvfhokutpovl","v":{"string":"kykfkitoqk"}},{"k":"omoeoo","v":{"string":"f"}}]}}}
    {"content":{"org.locationtech.DataObj":{"kvmap":[{"k":"jdfpnxtleoh","v":{"double":0.7748286862915655}},{"k":"bueqwtmesmeesthinscnreqamlwdxprseejpkrrljfhdkijosnogusomvmjkvbljrfjafhrbytrfayxhptfpcropkfjcgs","v":{"int":-1787843080}},{"k":"nmopnvrcjyar","v":null},{"k":"i","v":{"string":"hcslpunas"}}]}}}

Here's a more relevant sample record:

::

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

We can define a converter config to parse the Avro:

::

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

AvroPath
~~~~~~~~

GeoMesa Convert allows users to define "avropaths" to the data similar
to a jsonpath or xpath. This AvroPath allows you to extract out fields
from Avro records into SFT fields.

avroPath
^^^^^^^^

Description: Extract values from nested Avro structures.

Usage: ``avroPath($ref, $pathString)``

-  ``$ref`` - a reference object (avro root or extracted object)
-  ``pathString`` - forward-slash delimited path strings. paths are
   field names with modifiers:
-  ``$type=<typename>`` - interpret the field name as an avro schema
   type
-  ``[$<field>=<value>]`` - select records with a field named "field"
   and a value equal to "value"