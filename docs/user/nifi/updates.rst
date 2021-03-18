Record Update Processors
------------------------

GeoMesa provides processors for record-based updates to existing features (``UpdateGeoMesa*Record``). Compared to
the ingest processors in modify mode, only the fields in the record will be updated, while other fields in the
existing feature will be preserved. Record update processors accept the following configuration parameters for
specifying the input source. Each datastore-specific processor also has additional parameters for connecting to
the datastore, detailed in the following sections.

+-----------------------------------+-----------------------------------------------------------------------------------------------------+
| Property                          | Description                                                                                         |
+===================================+=====================================================================================================+
| ``Record reader``                 | The Record Reader to use for deserializing the incoming data                                        |
+-----------------------------------+-----------------------------------------------------------------------------------------------------+
| ``Feature type name``             | Name of the simple feature type schema to update. If not specified, will use the name               |
|                                   | from the record schema                                                                              |
+-----------------------------------+-----------------------------------------------------------------------------------------------------+
| ``Lookup column``                 | Column that will be used to match features for update                                               |
+-----------------------------------+-----------------------------------------------------------------------------------------------------+
| ``Feature ID column``             | Column that will be used as the feature ID                                                          |
+-----------------------------------+-----------------------------------------------------------------------------------------------------+
| ``Geometry columns``              | Column(s) that will be deserialized as geometries, and their type, as a                             |
|                                   | SimpleFeatureType specification string (e.g. ``the_geom:Point``)                                    |
+-----------------------------------+-----------------------------------------------------------------------------------------------------+
| ``Geometry serialization format`` | The format to use for serializing/deserializing geometries, either                                  |
|                                   | `WKT <https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry>`_ or                |
|                                   | `WKB <https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary>`_ |
+-----------------------------------+-----------------------------------------------------------------------------------------------------+
| ``Visibilities column``           | Column to use for feature visibilities (see :ref:`data_security`)                                   |
+-----------------------------------+-----------------------------------------------------------------------------------------------------+
