Avro Processors
---------------

GeoAvro processors (``AvroToPutGeoMesa*``) accept the following configuration parameters for specifying the
input source. Each datastore-specific processor also has additional parameters for connecting to the datastore,
detailed in the following sections.

+-----------------------------+-------------------------------------------------------------------------------------------+
| Property                    | Description                                                                               |
+=============================+===========================================================================================+
| ``FeatureNameOverride``     | Override the feature type name from the Avro file schema                                  |
+-----------------------------+-------------------------------------------------------------------------------------------+
| ``Use provided feature ID`` | Use the feature ID from the Avro file, or generate a new random feature ID                |
+-----------------------------+-------------------------------------------------------------------------------------------+
