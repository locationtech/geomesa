.. _nifi_record_input_configuration:

Record Processors
-----------------

The record-based processor (``PutGeoMesaRecord``) accepts the following configuration parameters for specifying
the input source:

+-----------------------------------+-----------------------------------------------------------------------------------------------------+
| Property                          | Description                                                                                         |
+===================================+=====================================================================================================+
| ``Record reader``                 | The Record Reader to use for deserializing the incoming data                                        |
+-----------------------------------+-----------------------------------------------------------------------------------------------------+
| ``Feature type name``             | Name to use for the simple feature type schema. If not specified, will use the name                 |
|                                   | from the record schema                                                                              |
+-----------------------------------+-----------------------------------------------------------------------------------------------------+
| ``Feature ID column``             | Column that will be used as the feature ID. If not specified, a random ID will be used              |
+-----------------------------------+-----------------------------------------------------------------------------------------------------+
| ``Geometry columns``              | Column(s) that will be deserialized as geometries, and their type, as a                             |
|                                   | SimpleFeatureType specification string (e.g. ``the_geom:Point``). A '*' can be used to              |
|                                   | indicate the default geometry column, otherwise it will be the first geometry in the schema         |
+-----------------------------------+-----------------------------------------------------------------------------------------------------+
| ``Geometry serialization format`` | The format to use for serializing/deserializing geometries, either                                  |
|                                   | `WKT <https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry>`_ or                |
|                                   | `WKB <https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary>`_ |
+-----------------------------------+-----------------------------------------------------------------------------------------------------+
| ``JSON columns``                  | Column(s) that contain valid JSON documents, comma-separated (must be STRING type columns)          |
+-----------------------------------+-----------------------------------------------------------------------------------------------------+
| ``Default date column``           | Column to use as the default date attribute (must be a DATE or TIMESTAMP type column)               |
+-----------------------------------+-----------------------------------------------------------------------------------------------------+
| ``Visibilities column``           | Column to use for feature visibilities (see :ref:`data_security`)                                   |
+-----------------------------------+-----------------------------------------------------------------------------------------------------+
| ``Schema user data``              | User data used to configure the GeoMesa SimpleFeatureType, in the form 'key1=value1,key2=value2'    |
+-----------------------------------+-----------------------------------------------------------------------------------------------------+

.. _geoavro_record_writer:

GeoAvroRecordSetWriterFactory
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

GeoMesa also provides a record writer that can be used to produce GeoAvro files from any NiFi processor that
supports record-based output. The writer factory uses the same properties detailed above.
