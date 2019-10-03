ADS-B Exchange (ADSBx)
======================

This directory provides `ADS-B Exchange <https://www.adsbexchange.com/>`__ GeoMesa ingest commands and converter configuration files.

ADSBx is the world's largest data source of unfiltered flight data.

Getting ADSBx data
------------------

ADSBx data can be downloaded by following the data retrieval steps found here: https://www.adsbexchange.com/data/

Ingest Commands
---------------

Check that ``adsbx`` simple feature type is available on the GeoMesa tools classpath. This is the default case.

    geomesa env | grep adsbx

If it is not, merge the contents of ``reference.conf`` with ``$GEOMESA_HOME/conf/application.conf``, or ensure that ``reference.conf`` is in ``$GEOMESA_HOME/conf/sfts/adsbx``

The default ingest use case is to ingest a text file where each line is a valid json object.

Converters
----------

The converter definitions provided include:
- adsbx-historical : For historical storage Ex. Accumulo, HBase, GeoMesa FSDS
- adsbx-live : For live storage Ex. Kafka, Redis

These extend from a common converter named ``adsbx-base`` and provide additional configuration.
For historical data storage, ``id-field`` is a unique concatenation formed from Icao, dtg, Lat, and Long.
For live storage, only Icao is used as the ``id-field`` which ensures that the most recently processed observation is correctly presented for that aircraft.
