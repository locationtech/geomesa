.. _confluent_kds:

Confluent Integration
=====================

.. warning::

  Confluent integration is currently experimental and supports consuming from Kafka, but not producing.

The Kafka Data Store can integrate with Confluent Kafka topics and the Confluent Schema Registry. The schema
registry is a centralized store of versioned Avro schemas, each associated with a particular Kafka topic. The
Confluent Kafka Data Store converts Avro schemas into ``SimpleFeatureTypes`` and deserializes records into
``SimpleFeatures``.

To read from a Confluent topic, set the URL to the schema registry in your data store parameter map under the key
``kafka.schema.registry.url``. In GeoServer, select the "Confluent Kafka (GeoMesa)" store instead of the
regular Kafka store.

Note that Confluent requires Avro 1.8 and the Confluent client JARs, which are not bundled with GeoMesa.

Supported Avro Schema Fields
----------------------------

The following avro schema field types are supported: ``STRING``, ``BOOLEAN``, ``INT``, ``DOUBLE``, ``LONG``, ``FLOAT``,
``BYTES``, and ``ENUM``. The ``UNION`` type is supported only for unions of ``NULL`` and one other supported type,
e.g. ``"type": ["null","string"]``.

Supported ``SimpleFeature`` Fields
----------------------------------

To encode types that may be part of a ``SimpleFeature``, but are not part of a standard Avro schema, e.g. ``Geometry``
or ``Date``, the Confluent Kafka Data Store supports interpreting several key-value metadata properties on schema
fields, which are described below. All property values are case insensitive.

Any additional properties on a field that are not listed below and are not standard avro properties will be included
as SFT attribute user data. Any additional properties on the schema will be included as SFT user data.

``geomesa.geom.format``
^^^^^^^^^^^^^^^^^^^^^^^

Indicates that the field should be interpreted as a ``Geometry`` in the given format. Must be accompanied by the key
``geomesa.geom.type``.

=========== ===================== ====================================================
Value       Schema Field Type     Description
=========== ===================== ====================================================
``wkt``     ``STRING``            Well-Known Text representation of a ``Geometry``
``wkb``     ``BYTES``             Well-Known Binary representation of a ``Geometry``
=========== ===================== ====================================================

``geomesa.geom.type``
^^^^^^^^^^^^^^^^^^^^^

Indicates that the field should be interpreted as a ``Geometry`` of the given type. Must be accompanied by the key
``geomesa.geom.format``.

======================== ============================
Value                    Description
======================== ============================
``Geometry``             A ``Geometry``
``Point``                A ``Point``
``LineString``           A ``LineString``
``Polygon``              A ``Polygon``
``MultiPoint``           A ``MultiPoint``
``MultiLineString``      A ``MultiLineString``
``MultiPolygon``         A ``MultiPolygon``
``GeometryCollection``   A ``GeometryCollection``
======================== ============================

``geomesa.geom.default``
^^^^^^^^^^^^^^^^^^^^^^^^

Indicates that the field represents the default ``Geometry`` for this ``SimpleFeatureType``. If the keys
``geomesa.geom.format`` and ``geomesa.geom.type`` are not present on the same schema field, this attribute will
be ignored. There may only be one of these properties for a given schema.

=========== ===============================
Value       Description
=========== ===============================
``true``    The default ``Geometry``
``false``   Not the default ``Geometry``
=========== ===============================

``geomesa.date.format``
^^^^^^^^^^^^^^^^^^^^^^^

Indicates that the field should be interpreted as a ``Date`` in the given format.

=========================== ===================== ====================================================
Value                       Schema Field Type     Description
=========================== ===================== ====================================================
``epoch-millis``            ``LONG``              Milliseconds since the Unix epoch
``iso-date``                ``STRING``            Generic ISO date format
``iso-datetime``            ``STRING``            Generic ISO datetime format
=========================== ===================== ====================================================

``geomesa.visibility.field``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies that the value of this field should be used as the visibility for features of this ``SimpleFeatureType``.
There may only be one of these properties for a given schema.

============= ===================== ========================================================
Value         Schema Field Type     Description
============= ===================== ========================================================
``true``      ``STRING``            Use the value of this field as the feature visibility
``false``     ``STRING``            Do not use this field as the feature visibility
============= ===================== ========================================================

``geomesa.exclude.field``
^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies whether this field should be excluded from the ``SimpleFeatureType``. All fields without this property will
be included.

=========== ===============================================
Value       Description
=========== ===============================================
``true``    Exclude this field field from the SFT
``false``   Do not exclude this field field from the SFT
=========== ===============================================

Example GeoMesa Avro Schema
---------------------------

::

    {
      "namespace": "org.locationtech",
      "type": "record",
      "name": "GeoMesaAvroSchema",
      "geomesa.index.dtg": "date",
      "fields": [
        {
          "name": "id",
          "type": "string",
          "index": "true",
          "cardinality": "high"
        },
        {
          "name": "position",
          "type": "string",
          "geomesa.geom.format": "wkt",
          "geomesa.geom.type": "point",
          "geomesa.geom.default": "true",
          "srid": "4326"
        },
        {
          "name": "timestamp",
          "type": ["null","long"],
          "geomesa.date.format": "epoch-millis"
        },
        {
          "name": "date",
          "type": "string",
          "geomesa.date.format": "iso-datetime"
        },
        {
          "name": "visibility",
          "type": "string",
          "geomesa.visibility.field": "true",
          "geomesa.exclude.field": "true"
        }
      ]
    }

Schema Overrides Config
-----------------------

The schema used to generate a ``SimpleFeatureType`` may optionally be overridden per topic by adding a data store
configuration parameter at the key ``kafka.schema.overrides``. The value must be a Typesafe Config string with the
top-level key ``schemas`` that is an object that contains a mapping from topic name to schema definition.
If an override for a schema exists, it will be used instead of the schema registry. The overrides might be useful
if you have an existing schema without the GeoMesa properties.

Schema Overrides Example Config
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

    {
      "schemas": {
        "topic1": {
          "type": "record",
          "name": "schema1",
          "fields": [
            {
              "name": "id",
              "type": "string",
              "cardinality": "high"
            },
            {
              "name": "position",
              "type": "string",
              "geomesa.geom.format": "wkt",
              "geomesa.geom.type": "point",
              "geomesa.geom.default": "true"
            },
            {
              "name": "speed",
              "type": "double"
            }
          ]
        },
        "topic2": {
          "type": "record",
          "name": "schema2",
          "fields": [
            {
              "name": "shape",
              "type": "bytes",
              "geomesa.geom.format": "wkb",
              "geomesa.geom.type": "geometry"
            },
            {
              "name": "date",
              "type": ["null","long"],
              "geomesa.date.format": "epoch-millis"
            }
          ]
        }
      }
    }
