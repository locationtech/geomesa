GeoMesa Web Data
================

The GeoMesa Web Data modules is found in ``geomesa-web/geomesa-web-data`` in
the source distribution.

.. warning::

    The GeoMesa Web Data module is deprecated, and will be removed in an upcoming release.

Building Instructions
---------------------

This project contains web services for accessing GeoMesa.

If you wish to build this project separately, you can with maven:

.. code-block:: shell

    geomesa> mvn clean install -pl geomesa-web/geomesa-web-data

Installation Instructions
-------------------------

To install geomesa-web-data, extract all jars from
``geomesa-web/geomesa-web-data/target/geomesa-web-data-<version>-install.tar.gz``
into your geoserver lib folder. If not already installed, you will need to install the
GeoMesa Accumulo plugin (see :ref:`install_accumulo_geoserver`).
