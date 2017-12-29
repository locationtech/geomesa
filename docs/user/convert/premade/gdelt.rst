.. _gdelt_converter:

Global Database of Events, Language, and Tone (GDELT)
=====================================================

This directory provides `GDELT <http://gdeltproject.org/>`__ GeoMesa
ingest commands and converter configuration files.

GDELT Event Database periodically scans news articles and uses natural
language processing to identify "the people, locations, organizations,
counts, themes, sources, emotions, quotes and events driving our
global society every second of every day."

GDELT data is updated each morning at 6am.

Getting GDELT data
------------------

GDELT has two different formats, the original 1.0 and the new 2.0 format. See
`the GDELT documentation, <https://www.gdeltproject.org/data.html#documentation>`__
for more information. GeoMesa provides simple feature types and converters for both,
named ``gdelt`` and ``gdelt2``, respectively.

The GDELT data set can be downloaded using the provided
``download-data.sh`` script in ``$GEOMESA_ACCUMULO_HOME/bin/`` as such

::

    ./download-data.sh gdelt

Alternatively, download the GDELT from `the GDELT events
page, <http://data.gdeltproject.org/events/index.html>`__ select the zip
file for the desired day, then ``unzip`` this in a convenient directory.
This will result in a single CSV file.

Be aware that these files are actually tab-delimited, but are given the
CSV extension for compatibility purposes. For this reason, be careful
when modifying and saving these files in software like Excel as commas
may be automatically inserted, breaking ingest functionality due to
records like "Baltimore, Maryland".

For more information on features of the GDELT data, see their
documentation
`here. <http://www.gdeltproject.org/data.html#documentation>`__

Ingest Commands
---------------

Check that the ``gdelt`` and ``gdelt2`` simple feature types are available on the GeoMesa
tools classpath. This is the default case.

::

    $ geomesa-accumulo env | grep gdelt

If it is not, merge the contents of ``reference.conf`` to
``$GEOMESA_ACCUMULO_HOME/conf/application.conf``, or ensure that
``reference.conf`` is in ``$GEOMESA_ACCUMULO_HOME/conf/sfts/gdelt``.

Run the ingest. You may optionally point to a different accumulo
instance using ``-i`` and ``-z`` options. See ``geomesa-accumulo help ingest``
for more detail.

::

    $ geomesa-accumulo ingest -u USERNAME -c CATALOGNAME -s gdelt -C gdelt gdelt_data.csv

Any errors in ingestion will be logged to ``$GEOMESA_ACCUMULO_HOME/logs``.
