.. _avro_schema_registry_converter:

Avro Schema Registry Converter
==============================

The Avro Schema Registry converter handles data written by `Apache Avro <http://avro.apache.org/>`__
using a Confluent Schema Registry. The schema registry is a centralized store of versioned Avro schemas.

To use the Avro converter, specify ``type = "avro-schema-registry"`` in your converter definition.

Note that Confluent requires Avro 1.8 and the Confluent client JARs, which are not bundled with GeoMesa.


Configuration
-------------

The Avro Schema Registry converter supports parsing Avro data using a Confluent schema registry.
To configure the schema registry set ``schema-registry = "<URL of schema registry>"`` in your converter definition.

The Avro record being parsed is available to field transforms as ``$1``.

The Avro Schema Registry Converter is an extension of the :ref:`avro_converter`, therefore the :ref:`avro_converter_functions`
can be used to extract fields out of the parsed Avro record.


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

Let's say we want to convert our Avro records into simple
features. We notice that between the two schema versions there are 3 attributes:

-  lat
-  lon
-  extra

The following converter config would be sufficient to parse the Avro records that have been encoded
using multiple schema version defined in the schema registry::

    {
      type        =     "avro-schema-registry"
      schema-registry = "http://localhost:8080"
      sft         =     "testsft"
      id-field    =     "uuid()"
      fields = [
        { name = "lat",    transform = "avroPath($1, '/lat')" },
        { name = "lon",    transform = "avroPath($1, '/lon')" },
        { name = "extra",  transform = "avroPath($1, '/extra')",
        { name = "geom",   transform = "point($lon, $lat)" }
      ]
    }

Note that in the simple feature, the ``extra`` field will be null for Avro records encoded using
schema version 1 and will be populated for records encoded using schema version 2.
