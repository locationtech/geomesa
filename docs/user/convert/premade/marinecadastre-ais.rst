Marine Cadastre AIS
====================

`Automated Identification System <https://en.wikipedia.org/wiki/Automatic_identification_system>`_ is a global system to
track ships, with each vessel beaconing its position to other (nearby) vessels. While various commercial data providers
exist, this converter is intended for data collected by the U.S. Coast Guard around the USA and openly published by
`Marine Cadastre <https://marinecadastre.gov/ais/>`_.

Prior to 2015, the data is disseminated as zipped ESRI File Geodatabases containing a number of tables. Only the
``broadcast`` table is considered here, which consists of vessel locations over time. Note that "Ship name and call sign
fields have been removed, and the MMSI (Maritime Mobile Service Identity) field has been encrypted for the 2010 through
2014 data at the request of the U.S. Coast Guard."

For 2015 and later, the data is disseminated as zipped Comma Separated Values (CSV) files which have been enriched with
additional vessel data (e.g., vessel name) and include unencrypted MMSI values.

Getting the Data
----------------

Machine-friendly links for bulk data download can be found
`here <https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2009/index.html>`__. Replace the four digit year with that
desired (2009-2017), data is then split into month and
`UTM Zone <https://marinecadastre.gov/ais/AIS%20Documents/UTMZoneMap2014.png>`__. Once data is downloaded, it must be
unzipped e.g.

::

    find . -name "*.zip" -exec unzip {} \;

Converting the pre-2015 Data
----------------------------

GeoMesa does not currently support ESRI File Geodatabases (FileGDB), so an external tool is required to convert the data
into a suitable format. ``ogr2ogr`` from `GDAL <https://www.gdal.org/>`__ can convert from FileGDB into Comma Separated
Value (CSV) format e.g.

::

    find . -name "*.gdb" -exec sh -c "ogr2ogr -f CSV /dev/stdout {} Broadcast -lco GEOMETRY=AS_XY | tail -n +2 > {}.csv" \;

Note that the ``tail`` command removes the header row from the output file, making ingest more amenable to processing
using HDFS or similar. Also note that the resulting CSV files may be very large -- 264 GB for the 2009 & 2019 data.

Sample Ingest Command
----------------------

Due to the two different formats, two different simple feature types (SFTs) and converters are required, both of which
are defined in the same file. ``marinecadastre-ais`` defines the SFT and converter for data converted to CSV from
FileGDB format (prior to 2015), while ``marinecadastre-ais-csv`` defines the SFT and converter for data natively in the
newer CSV format (2015 and later).

Check that the desired simple feature type and converter are available on the GeoMesa tools classpath.
This is the default case. Note that you will need to use the command specific to your back-end e.g.
``geomesa-accumulo``.

::

    geomesa env | grep 'marinecadastre-ais'

If they are not, merge the contents of ``reference.conf`` with ``$GEOMESA_HOME/conf/application.conf``, or ensure that
``reference.conf`` is in ``$GEOMESA_HOME/conf/sfts/marinecadastre-ais``.

To ingest using the GeoMesa command line interface:

::

    $ geomesa ingest -u username -c catalogName -s marinecadastre-ais -C marinecadastre-ais -t 8 /path/to/data/*.csv

You should replace ``marinecadastre-ais`` with ``marinecadastre-ais-csv`` if using 2015 or later data.

This example uses 8 threads, which for all of the 2009 & 2010 data (approx 3.5B records) took 15h on a 5 node cluster.
