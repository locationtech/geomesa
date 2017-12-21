GeoNames
========

This directory provides `GeoNames <http://www.geonames.org/>`__ GeoMesa
ingest commands and converter configuration files.

GeoNames is a geographical database containing over 10 million
geographical names and over 9 million unique features. All features are
classified as one of nine feature codes and then again sub-categorized
into one of 645 feature codes. This data is freely available directly
from GeoNames's website.

Getting GeoNames data
---------------------

The GeoNames data set can be downloaded using the provided
``download-data.sh`` script in ``$GEOMESA_ACCUMULO_HOME/bin/`` as such

::

    ./download-data.sh geonames

providing a desired country code when prompted.

Alternatively, GeoNames data can be downloaded from `GeoNames's
Server <http://download.geonames.org/export/dump/>`__. Files are
downloaded in .zip format and need to be unzipped to a text document
before being ready for use. The entire database can be ingested using
``allCountries.txt`` or a subset (such as ``cities15000.txt``) can be
downloaded instead.

Ingest Commands
---------------

Check that the ``geonames`` simple feature type is available on the GeoMesa
tools classpath. This is the default case.

::

    $ geomesa-accumulo env | grep geonames

If it is not, merge the contents of ``reference.conf`` with
``$GEOMESA_ACCUMULO_HOME/conf/application.conf``, or ensure that
``reference.conf`` is in ``$GEOMESA_ACCUMULO_HOME/conf/sfts/geonames``

Run the ingest. You may optionally point to a different accumulo
instance using ``-i`` and ``-z`` options. See ``geomesa-accumulo help ingest``
for more detail. The most important detail is referencing the
``geonames`` SimpleFeatureType and ``geonames`` converter.

::

    $ geomesa-accumulo ingest -u USERNAME -c CATALOGNAME -s geonames -C geonames cities15000.txt
