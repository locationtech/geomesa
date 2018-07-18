Loading Converters and SFTs at Runtime
--------------------------------------

If you have defined converters or SimpleFeatureTypes in Typesafe Config you can place
them on the classpath or load them with a ``ConverterConfigProvider`` or
``SimpleFeatureTypeProvider`` via Java SPI loading. By default, classpath
and URL providers are provided. Placing a Typesafe Config file named
``reference.conf`` or ``application.conf`` containing properly formatted converters and SFTs
(see the example ``application.conf`` in :doc:`./usage_tools`) in a JAR file on the
classpath will enable the reference of the converters and SFTs using the public loader
API:

.. code-block:: scala

    // ConverterConfigLoader.scala
    // Public API
    def listConverterNames: List[String] = confs.keys.toList
    def getAllConfigs: Map[String, Config] = confs
    def configForName(name: String) = confs.get(name)

    // SimpleFeatureTypeLoader.scala
    // Public API
    def listTypeNames: List[String] = sfts.map(_.getTypeName)
    def sftForName(n: String): Option[SimpleFeatureType] = sfts.find(_.getTypeName == n)

.. note::

    The prepackaged SFTs and converters for public data sources provided with GeoMesa binary
    distributions are also available as an artifact that may be obtained via Maven or SBT. See
    :ref:`prepackaged_converters` for more information.
