.. _confluent_kds:

Confluent Integration
=====================

.. warning::

  Confluent integration is currently experimental and supports consuming from Kafka, but not producing.

The Kafka Data Store can integrate with Confluent Kafka topics and the Confluent Schema Registry. The schema
registry is a centralized store of versioned Avro schemas, each associated with a particular Kafka topic. The
``ConfluentKafkaDataStore`` can convert an Avro schema into a ``SimpleFeatureType`` and seamlessly deserialize
records into ``SimpleFeatures``.

To read from a Confluent topic, set the URL to the schema registry in your data store parameter map under the key
``kafka.schema.registry.url``. In GeoServer, select the "Confluent Kafka (GeoMesa)" store instead of the
regular Kafka store.

Note that Confluent requires Avro 1.8 and the Confluent client JARs, which are not bundled with GeoMesa.

Supported Avro Schema Fields
----------------------------

The following avro schema field types are supported: ``STRING``, ``BOOLEAN``, ``INT``, ``DOUBLE``, ``LONG``, ``FLOAT``,
``BYTES``, and ``ENUM``. The ``UNION`` type is supported only for unions of null and one other supported type,
e.g. ``"type": ["null","string"]``.

Supported ``SimpleFeature`` Fields
----------------------------------

To encode types that may be part of a ``SimpleFeature``, but are not part of a standard Avro schema, e.g. ``Geometry``
or ``Date``, the Confluent Kafka Data Store supports interpreting several key-value metadata attributes on schema
fields, which are described below.

All the attribute values are case insensitive.

``geomesa.geom.format``
^^^^^^^^^^^^^^^^^^^^^^^

Indicates that this field should be interpreted as a ``Geometry`` in the given format. Must be accompanied by the key
``geomesa.geom.type``.

=========== ===================== ====================================================
Value       Schema Field Type     Description
=========== ===================== ====================================================
``WKT``     ``STRING``            Well-Known Text representation of a ``Geometry``
``WKB``     ``BYTES``             Well-Known Binary representation of a ``Geometry``
=========== ===================== ====================================================

``geomesa.geom.type``
^^^^^^^^^^^^^^^^^^^^^^^

Indicates that this field should be interpreted as a ``Geometry`` of the given type. Must be accompanied by the key
``geomesa.geom.format``.

======================== ============================
Value                    Description
======================== ============================
``GEOMETRY``             A ``Geometry``
``POINT``                A ``Point``
``LINESTRING``           A ``LineString``
``POLYGON``              A ``Polygon``
``MULTIPOINT``           A ``MultiPoint``
``MULTILINESTRING``      A ``MultiLineString``
``MULTIPOLYGON``         A ``MultiPolygon``
``GEOMETRYCOLLECTION``   A ``GeometryCollection``
======================== ===========================

``geomesa.geom.default``
^^^^^^^^^^^^^^^^^^^^^^^^

Indicates that this field should be interpreted as a the default ``Geometry`` for this ``SimpleFeatureType``. If the
keys ``geomesa.geom.format`` and ``geomesa.geom.type`` are not present on the same schema field, this attribute will
be ignored.

=========== ===============================
Value       Description
=========== ===============================
``TRUE``    The default ``Geometry``
``FALSE``   Not the default ``Geometry``
=========== ===============================

``geomesa.date.format``
^^^^^^^^^^^^^^^^^^^^^^^

Indicates that this field should be interpreted as a ``Date`` in the given format.

================== ===================== ====================================================
Value              Schema Field Type     Description
================== ===================== ====================================================
``EPOCH_MILLIS``   ``LONG``              Milliseconds since the Unix epoch
``ISO_DATE``       ``STRING``            Date format "yyyy-MM-dd"
``ISO_INSTANT``    ``STRING``            Date format "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"
================== ===================== ====================================================

``geomesa.feature.visibility``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Indicates that this field should be used as the visibility attribute for ``SimpleFeatures`` of this
``SimpleFeatureType``. The field must have ``STRING`` type. Any string value for this key will activate the attribute.

Example Schema
--------------

::

    {
      "namespace": "org.locationtech",
      "type": "record",
      "name": "SchemaRegistryMessage",
      "fields": [
        {
          "name": "position",
          "type": "string",
          "geomesa.geom.format": "WKT",
          "geomesa.geom.type": "POINT",
          "geomesa.geom.default": "TRUE"
        },
        {
          "name": "date",
          "type": ["null","long"],
          "geomesa.date.format": "EPOCH_MILLIS"
        },
        {
          "name": "visibility",
          "type": "string",
          "geomesa.feature.visibility": ""
        }
      ]
    }
