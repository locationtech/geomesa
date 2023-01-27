T-Drive Trajectory Data
=======================

This directory provides
`T-Drive <https://research.microsoft.com/apps/pubs/?id=152883>`__ GeoMesa
ingest commands and converter configuration files.

T-Drive is a project of Microsoft Research Asia. The `overall
project <https://research.microsoft.com/en-us/projects/tdrive/>`__
collected GPS tracks from over 30,000 taxis in Beijing for three months.
These data were used to demonstrate a more efficient routing system.

Microsoft makes a small subsample of its data `available for
download <https://research.microsoft.com/apps/pubs/?id=152883>`__. This
is about one third of the taxis for a period of a week.

Before proceeding please check out the data description and included
`terms of use <https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/User_guide_T-drive.pdf>`__.

Getting T-Drive data
--------------------

The T-Drive data set can be downloaded using the provided
``download-data.sh`` script in ``$GEOMESA_ACCUMULO_HOME/bin/`` as such

::

    ./download-data.sh tdrive

Alternatively, download the T-Drive datasets from
`here <https://research.microsoft.com/apps/pubs/?id=152883>`__ download
one or all of the zip files, then ``unzip`` into a convenient directory.

Each zip contains several hundred CSVs for a total of 7,952 files and
nearly 15 million data points.

Each CSV is formatted as follows: > Taxi Identifier, Timestamp,
Longitude, Latitude Example: > 102, 2008-02-02
13:34:27,116.30826,39.94702

Ingest Commands
---------------

Check that the ``tdrive`` simple feature type is available on the GeoMesa
tools classpath. This is the default case.

::

    $ geomesa-accumulo env | grep tdrive

If it is not, merge the contents of ``reference.conf`` with
``$GEOMESA_ACCUMULO_HOME/conf/application.conf``, or ensure that
``reference.conf`` is in ``$GEOMESA_ACCUMULO_HOME/conf/sfts/tdrive``.

Run the ingest. You may optionally point to a different accumulo
instance using ``-i`` and ``-z`` options. See ``geomesa-accumulo help ingest``
for more detail.

::

    $ geomesa-accumulo ingest -u USERNAME -c CATALOGNAME -s tdrive -C tdrive tdrive_data.txt

Any errors during the ingest will be logged to ``$GEOMESA_ACCUMULO_HOME/logs``.
