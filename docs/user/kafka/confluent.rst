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
``geometry``             A ``Geometry``
``point``                A ``Point``
``linestring``           A ``LineString``
``polygon``              A ``Polygon``
``multipoint``           A ``MultiPoint``
``multilinestring``      A ``MultiLineString``
``multipolygon``         A ``MultiPolygon``
``geometrycollection``   A ``GeometryCollection``
======================== ============================

``geomesa.geom.default``
^^^^^^^^^^^^^^^^^^^^^^^^

Indicates that the field represents the default ``Geometry`` for this ``SimpleFeatureType``. If the keys
``geomesa.geom.format`` and ``geomesa.geom.type`` are not present on the same schema field, this attribute will
be ignored.

=========== ===============================
Value       Description
=========== ===============================
``true``    The default ``Geometry``
``false``   Not the default ``Geometry``
=========== ===============================

``geomesa.geom.srid``
^^^^^^^^^^^^^^^^^^^^^

Indicates that the field represents a ``[[Geometry]]`` with the given spatial reference identifier (SRID).
If the keys ``geomesa.geom.format`` and ``geomesa.geom.type`` are not present on the same schema field, this
attribute will be ignored.

=========== ==============================================================
Value       Description
=========== ==============================================================
``4326``    Longitude/latitude coordinate system on the WGS84 ellipsoid
=========== ==============================================================

``geomesa.date.format``
^^^^^^^^^^^^^^^^^^^^^^^

Indicates that the field should be interpreted as a ``Date`` in the given format.

=========================== ===================== ====================================================
Value                       Schema Field Type     Description
=========================== ===================== ====================================================
``epoch-millis``            ``LONG``              Milliseconds since the Unix epoch
``iso-date``                ``STRING``            Date format ``yyyy-MM-dd``
``iso-instant``             ``STRING``            Date format ``yyyy-MM-dd'T'HH:mm:ss.SSSZZ``
``iso-instant-no-millis``   ``STRING``            Date format ``yyyy-MM-dd'T'HH:mm:ssZZ``
=========================== ===================== ====================================================

``geomesa.date.default``
^^^^^^^^^^^^^^^^^^^^^^^^

Indicates that the field represents the default ``Date`` for this ``SimpleFeatureType``. If the key
``geomesa.date.format`` is not present on the same schema field, this attribute will be ignored.

=========== =============================
Value       Description
=========== =============================
``true``    The default ``Date``
``false``   Not the default ``Date``
=========== =============================

``geomesa.index``
^^^^^^^^^^^^^^^^^

Indicates that the field should be indexed by GeoMesa.

=========== ==================================
Value       Description
=========== ==================================
``true``    Index this attribute
``false``   Do not index this attribute
``full``    Index the full ``SimpleFeature``
=========== ==================================

``geomesa.cardinality``
^^^^^^^^^^^^^^^^^^^^^^^

Specifies a cardinality hint for the attribute, for use by GeoMesa in query planning. This property should be used
in conjunction with ``geomesa.index``.

=========== ====================================
Value       Description
=========== ====================================
``high``    Prioritize this attribute index
``low``     De-prioritize this attribute index
=========== ====================================

``geomesa.avro.visibility.field``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies a field in the Avro schema which should be used as the visibility for features of the ``SimpleFeatureType``.

============= ===================== ========================================================
Value         Schema Field Type     Description
============= ===================== ========================================================
``true``      ``STRING``            Use the value of this field as the feature visibility
``false``     ``STRING``            Do not use this field as the feature visibility
============= ===================== ========================================================

Example Schema
--------------

Any additional top-level properties on the schema will be included in the SFT as user data.

::

    {
      "namespace": "org.locationtech",
      "type": "record",
      "name": "SchemaRegistryMessage",
      "geomesa.table.sharing" = "false",
      "geomesa.table.compression.enabled" = "true",
      "fields": [
        {
          "name": "id",
          "type": "string",
          "geomesa.index": "full",
          "geomesa.cardinality": "high"
        },
        {
          "name": "position",
          "type": "string",
          "geomesa.geom.format": "wkt",
          "geomesa.geom.type": "point",
          "geomesa.geom.default": "true",
          "geomesa.geom.srid": "4326"
        },
        {
          "name": "date",
          "type": ["null","long"],
          "geomesa.date.format": "epoch-millis"
        }
      ]
    }
