.. _prepackaged_converters:

Prepackaged Converter Definitions
=================================

GeoMesa Tools distributions contain several prepackaged converters for common, publicly
available data sources. These converters are bundled with the command-line tools
in the GeoMesa binary distributions, in individual directories under ``conf/sfts``,
which are included by the ``conf/reference.conf`` file.

These prepackaged converters can also be obtained in a JAR that can be placed on the classpath
of your project. This JAR may be obtained via Maven or SBT by requesting the ``geomesa-tools_2.11``
artifact with the ``data`` classifier. For example, for Maven add the following to your POM:

.. code-block:: xml

    <dependency>
        <groupId>org.locationtech.geomesa</groupId>
        <artifactId>geomesa-tools_2.11</artifactId>
        <version>${geomesa.version}</version>
        <classifier>data</classifier>
    </dependency>

.. note::

    The examples in the sections below are often specific to the GeoMesa Accumulo distribution,
    but the general principle is the same for each distribution. Only the home variable
    (e.g. ``$GEOMESA_ACCUMULO_HOME``) and command-line tool name will differ depending on
    GeoMesa distribution.

.. toctree::
    :maxdepth: 1

    gdelt
    geolife
    geonames
    gtd
    nyctaxi
    osm
    osm-gpx
    marinecadastre-ais
    tdrive
    twitter