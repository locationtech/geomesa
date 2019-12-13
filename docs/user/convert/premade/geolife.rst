GeoLife GPS Trajectory Data
===========================

This directory provides
`GeoLife <http://research.microsoft.com/en-us/projects/geolife/>`__
GeoMesa ingest commands and converter configuration files.

The GeoLife dataset contain timestamped latitude, longitude, and
altitude for 182 different users. There are 17,621 total points,
spanning 1.2 kilometers and years 2007-2012.

Data was collected from users' phones and other GPS loggers and is meant
to model activity such as commuting, shopping, sightseeing, etc.

Some user entries contain additional information about the mode of
transportation.

Getting GeoLife data
--------------------

The GeoLife data set can be downloaded using the provided
``download-data.sh`` script in ``$GEOMESA_ACCUMULO_HOME/bin/`` as such

::

    ./download-data.sh geolife

Alternatively, download the GeoLife Data
`here <http://research.microsoft.com/en-us/downloads/b16d359d-d164-469e-9fd4-daa38f2b2e13/>`__.

Once you have downloaded the data, extract the ZIP file for ingestion.

.. note::

    Make sure that you maintain the directory structure of the data set. The ingest
    converter expects this hierarchy in order to correctly parse
    the user ID and the track ID of each trace.

Data Format
-----------

Each folder of the dataset represents one user's GPS logs. Each log is
formatted as a PLT file as follows (based on the user guide contained in
the zip file):

    Line 1...6 are useless in this dataset, and can be ignored. Points
    are described in following lines, one for each line.

    | Field 1: Latitude in decimal degrees.
    | Field 2: Longitude in decimal degrees.
    | Field 3: All set to 0 for this dataset.
    | Field 4: Altitude in feet (-777 if not valid).
    | Field 5: Date - number of days (with fractional part) that have
      passed since 12/30/1899.
    | Field 6: Date as a string.
    | Field 7: Time as a string.
    | Note that field 5 and field 6&7 represent the same date/time in
      this dataset. You may use either of them.
    | Example:
      39.906631,116.385564,0,492,40097.5864583333,2009-10-11,14:04:30
      39.906554,116.385625,0,492,40097.5865162037,2009-10-11,14:04:35

More information on features of the GeoLife data, see their `documentation`_ as well as the user
guide contained in the zip of the dataset.

.. _documentation: http://research.microsoft.com/en-us/downloads/b16d359d-d164-469e-9fd4-daa38f2b2e13/

Ingesting the Data
------------------

Check that the ``geolife`` simple feature type is available on the GeoMesa
tools classpath. This is the default case.

::

    $ geomesa-accumulo env | grep geolife

If it is not, merge the contents of ``reference.conf`` with
``$GEOMESA_ACCUMULO_HOME/conf/application.conf``, or ensure that
``reference.conf`` is in ``$GEOMESA_ACCUMULO_HOME/conf/sfts/geolife``

Ensure that the extracted GeoLife data is in its original folder structure. This is
required for the converter to parse the user and track ID for each trace.

Run the ingest. You may optionally point to a different accumulo
instance using ``-i`` and ``-z`` options. See ``geomesa-accumulo help ingest``
for more detail.

::

    $ geomesa-accumulo ingest -u USERNAME -c CATALOGNAME -s geolife -C geolife \
      --threads 4 /path/to/Geolife\ Trajectories\ 1.3/Data/**/*.plt


Any errors in ingestion will be logged to ``$GEOMESA_ACCUMULO_HOME/logs``.

Citation
--------

Microsoft Research asks that you cite the following papers when using this dataset:

#. Yu Zheng, Lizhu Zhang, Xing Xie, Wei-Ying Ma. Mining interesting locations and travel sequences from GPS trajectories. In Proceedings of International conference on World Wild Web (WWW 2009), Madrid Spain. ACM Press: 791-800.
#. Yu Zheng, Quannan Li, Yukun Chen, Xing Xie, Wei-Ying Ma. Understanding Mobility Based on GPS Data. In Proceedings of ACM conference on Ubiquitous Computing (UbiComp 2008), Seoul, Korea. ACM Press: 312-321.
#. Yu Zheng, Xing Xie, Wei-Ying Ma, GeoLife: A Collaborative Social Networking Service among User, location and trajectory. Invited paper, in IEEE Data Engineering Bulletin. 33, 2,2010, pp. 32-40.
