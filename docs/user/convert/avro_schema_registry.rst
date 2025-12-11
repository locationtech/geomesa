.. _avro_schema_registry_converter:

Avro Schema Registry Converter
==============================

The Avro schema registry converter handles data written by `Apache Avro <https://avro.apache.org/>`__
using a Confluent schema registry. The schema registry is a centralized store of versioned Avro schemas.

Note that the schema registry converter requires Confluent client JARs, which are not bundled by default with GeoMesa.

Configuration
-------------

The Avro schema registry converter supports the following configuration keys:

===================== ======== ======= ==========================================================================================
Key                   Required Type    Description
===================== ======== ======= ==========================================================================================
``type``              yes      String  Must be the string ``avro-schema-registry``.
``schema-registry``   yes      String  URL of the schema registry.
===================== ======== ======= ==========================================================================================

Transform Functions
-------------------

The current Avro record being parsed is available to field transforms as ``$1``. The original message bytes are available
as ``$0``, which may be useful for generating consistent feature IDs.

The Avro schema registry converter is an extension of the :ref:`avro_converter`, therefore both the standard
:ref:`converter_functions` and the :ref:`avro_converter_functions` can be used to extract fields out of the parsed Avro record.

Example Usage
-------------

For this example we'll assume the following Avro schema is registered in the schema registry as version 1:

::

    {
      "namespace": "org.locationtech",
      "type": "record",
      "name": "SchemaRegistryMessageV1",
      "fields": [
        {
          "name": "lat",
          "type": "Double"
        },
        {
          "name": "lon",
          "type": "Double"
        }
      ]
    }

We'll also assume the following Avro schema is registered in the schema registry as version 2:

::

    {
      "namespace": "org.locationtech",
      "type": "record",
      "name": "SchemaRegistryMessageV2",
      "fields": [
        {
          "name": "lat",
          "type": "Double"
        },
        {
          "name": "lon",
          "type": "Double"
        },
        {
          "name": "extra",
          "type": "String"
        }
      ]
    }

Below is a sample Avro record encoded using schema version 1: ::

    {
      "lat": 45.0,
      "lon": 45.0
    }

Here's a sample Avro record encoded using schema version 2: ::

    {
      "lat": 45.0,
      "lon": 45.0,
      "extra": "Extra Test Field"
    }

Let's say we want to convert our Avro records into simple features. We notice that between the two schema versions there are
3 attributes:

-  lat
-  lon
-  extra

The following converter config would be sufficient to parse the Avro records that have been encoded
using multiple schema version defined in the schema registry::

    {
      type = "avro-schema-registry"
      schema-registry = "http://localhost:8080"
      id-field = "uuid()"
      fields = [
        { name = "lat",    transform = "avroPath($1, '/lat')" },
        { name = "lon",    transform = "avroPath($1, '/lon')" },
        { name = "extra",  transform = "avroPath($1, '/extra')",
        { name = "geom",   transform = "point($lon, $lat)" }
      ]
    }

Note that in the simple feature, the ``extra`` field will be null for Avro records encoded using
schema version 1 and will be populated for records encoded using schema version 2.
