Avro Processors
---------------

GeoAvro processors (``AvroToPutGeoMesa*``) accept the following configuration parameters for specifying the
input source. Each datastore-specific processor also has additional parameters for connecting to the datastore,
detailed in the following sections.

+-----------------------------+-------------------------------------------------------------------------------------------+
| Property                    | Description                                                                               |
+=============================+===========================================================================================+
| ``SftName``                 | Name of the SFT on the classpath to use. This property overrides SftSpec.                 |
+-----------------------------+-------------------------------------------------------------------------------------------+
| ``SftSpec``                 | SFT specification String. Overridden by SftName if both are set.                          |
+-----------------------------+-------------------------------------------------------------------------------------------+
| ``FeatureNameOverride``     | Override the feature type name from the Avro file schema                                  |
+-----------------------------+-------------------------------------------------------------------------------------------+
| ``Use provided feature ID`` | Use the feature ID from the Avro file, or generate a new random feature ID                |
+-----------------------------+-------------------------------------------------------------------------------------------+

The ``SftName``, ``SftSpec`` and ``FeatureNameOverride`` properties are optional. If not specified, the schema
from the Avro file will be used.
