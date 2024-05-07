.. _nifi_converter_processors:

Converter Processors
--------------------

Converter processors (``PutGeoMesa`` and ``ConvertToGeoFile``) accept the following configuration parameters for
specifying the input source:

+-------------------------------+-----------------------------------------------------------------------------------------+
| Property                      | Description                                                                             |
+===============================+=========================================================================================+
| ``SftName``                   | Name of the SFT on the classpath to use. This property overrides SftSpec.               |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``SftSpec``                   | SFT specification String. Overridden by SftName if both are set.                        |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``FeatureNameOverride``       | Override the feature type name on ingest from SftName or SftSpec.                       |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``ConverterName``             | Name of converter on the classpath to use. This property overrides ConverterSpec.       |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``ConverterSpec``             | Converter specification string. Overridden by ConverterName if both are set.            |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``ConverterErrorMode``        | Override the converter error mode (``log-errors`` or ``raise-errors``)                  |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``ConverterMetricReporters``  | Override the converter metrics reporters (see below)                                    |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``ConvertFlowFileAttributes`` | Expose flow file attributes to the converter framework, referenced by name              |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``ExtraClasspaths``           | Additional resources to add to the classpath, e.g. converter and SFT definitions        |
+-------------------------------+-----------------------------------------------------------------------------------------+

Additionally, the ``PutGeoMesa`` processor accepts additional configuration:

+-------------------------------+-----------------------------------------------------------------------------------------+
| Property                      | Description                                                                             |
+===============================+=========================================================================================+
| ``InitializeSchemas``         | Initialize schemas in the underlying data store when the processor is started. Schemas  |
|                               | should be defined in standard Java properties format, with the type name as the key,    |
|                               | and the feature type specification or lookup as the value                               |
+-------------------------------+-----------------------------------------------------------------------------------------+

Defining SimpleFeatureTypes and Converters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The GeoMesa NiFi processors package a set of predefined SimpleFeatureType schema definitions and GeoMesa
converter definitions for popular data sources such as Twitter, GDelt and OpenStreetMaps.

The full list of provided sources can be found in :ref:`prepackaged_converters`.

For custom data sources, there are two ways of providing custom SFTs and converters:

Providing SimpleFeatureTypes and Converters on the Classpath
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To bundle configuration in a JAR file simply place your config in a file named ``reference.conf`` and place it **at
the root level** of a JAR file:

.. code-block:: bash

    $ jar cvf data-formats.jar reference.conf

You can verify your JAR was built properly:

.. code-block:: bash

    $ jar tvf data-formats.jar
         0 Mon Mar 20 18:18:36 EDT 2017 META-INF/
        69 Mon Mar 20 18:18:36 EDT 2017 META-INF/MANIFEST.MF
     28473 Mon Mar 20 14:49:54 EDT 2017 reference.conf

Use the ``ExtraClasspaths`` property to point your processor to the JAR file. The property takes a list of
comma-delimited resources. Once set, the ``SftName`` and/or ``ConverterName`` properties will update with the
name of your converters. You will need to close the configuration panel and re-open it in order for the
properties to update.

Defining SimpleFeatureTypes and Converters via the UI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You may also provide SimpleFeatureTypes and Converters directly in the Processor configuration via the NiFi UI.
Simply paste your TypeSafe configuration into the ``SftSpec`` and ``ConverterSpec`` property fields.

Defining SimpleFeatureTypes and Converters via Flow File Attributes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You may also override the Processor configuration fields with flow file attributes. The following attributes
are available:

* ``geomesa.sft.name`` corresponds to the Processor configuration ``FeatureNameOverride``
* ``geomesa.sft.spec`` corresponds to the Processor configuration ``SftSpec``
* ``geomesa.converter`` corresponds to the Processor configuration ``ConverterSpec``

.. warning::

    Configuration via flow file attributes should be used with care, as any misconfigurations may multiply.
    For example, setting ``geomesa.sft.name`` to a non-recurring value could end up creating a new schema for each
    flow file, potentially crashing your database by creating too many tables.

Initializing Schemas
~~~~~~~~~~~~~~~~~~~~

The ``InitializeSchemas`` configuration can be used to create schemas in the underlying data store when the
processor is started, instead of having to wait for data to flow through the processor. The schemas are defined
as a standard Java properties file, where each line of the configuration should contain the name of a feature type
as the key and the definition of the type as the value, corresponding to ``FeatureNameOverride`` and
``SftName`` / ``SftSpec`` (see above). For example::

    gdelt=gdelt2
    example-csv=example-csv
    test=name:String,dtg:Date,*geom:Point:srid=436

Converter Metrics
~~~~~~~~~~~~~~~~~

GeoMesa supports publishing metrics on the ingest conversion process. See :ref:`converter_metrics` and
:ref:`geomesa_metrics` for details. The GeoMesa NiFi converter processors allow the metrics reporters to be
configured directly in NiFi with the ``ConverterMetricReporters`` property, instead of in the converter definition.
The property expects a TypeSafe Config block defining the reporters or list of reporters, for example:


::

  {
    type     = "slf4j"
    units    = "milliseconds"
    interval = "60 seconds"
    logger   = "org.locationtech.geomesa.metrics"
    level    = "debug"
  }
