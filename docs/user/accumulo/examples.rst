Command Line Examples
=====================

This chapter provides hands-on examples of some
common tasks in GeoMesa, including the management of registered feature types
in a data store, ingest of data, and export of data in a variety of formats.

Feature Type Management
-----------------------

Creating a feature type
^^^^^^^^^^^^^^^^^^^^^^^

To begin, let's start by creating a new feature type in GeoMesa with the
``create`` command. The ``create`` command takes three required and one
optional flag:

**Required**

-  ``-c`` or ``--catalog``: the name of the catalog table
-  ``-f`` or ``--feature-name``: the name of the feature
-  ``-s`` or ``--spec``: the ``SimpleFeatureType`` specification

**Optional**

-  ``--dtg``: the default date attribute of the
   ``SimpleFeatureType``

Run the command:

.. code:: bash

    $ geomesa-accumulo create-schema -u <username> -p <password> \
    -c cmd_tutorial \
    -f feature \
    -s fid:String:index=true,dtg:Date,geom:Point:srid=4326 \
    -dtg dtg

This will create a new feature type, named "feature", on the GeoMesa
catalog table "cmd\_tutorial". The catalog table stores metadata
information about each feature, and it will be used to prefix each table
name in Accumulo.

If the above command was successful, you should see output similar to
the following:

.. code:: bash

    Creating 'cmd_tutorial_feature' with spec 'fid:String:index=true,dtg:Date,geom:Point:srid=4326'. Just a few moments...
    Feature 'cmd_tutorial_feature' with spec 'fid:String:index=true,dtg:Date,geom:Point:srid=4326' successfully created.

Now that you've seen how to create feature types, create another feature
type on catalog table "cmd\_tutorial" using your own first name for the
``--feature-name`` and the above schema for the ``--spec``.

Listing known feature types
^^^^^^^^^^^^^^^^^^^^^^^^^^^

You should have two feature types on catalog table "cmd\_tutorial". To
verify, we'll use the ``list`` command. The ``list`` command takes one
flag:

-  ``-c`` or ``--catalog``: the name of the catalog table

Run the following command:

.. code:: bash

    $ geomesa-accumulo get-type-names -u <username> -p <password> -c cmd_tutorial

The output text should be something like:

.. code:: bash

    Listing features on 'cmd_tutorial'. Just a few moments...
    2 features exist on 'cmd_tutorial'. They are:
    feature
    gdelt

Finding the attributes of a feature type
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To find out more about the attributes of a feature type, we'll use the
``describe`` command. This command takes two flags:

-  ``-c`` or ``--catalog``: the name of the catalog table
-  ``-f`` or ``--feature-name``: the name of the feature type

Let's find out more about the attributes on our first feature type. Run
the command

.. code:: bash

    $ geomesa-accumulo describe-schema -u <username> -p <password> -c cmd_tutorial -f feature

The output should look like:

.. code:: bash

    Describing attributes of feature 'cmd_tutorial_feature'. Just a few moments...
    fid: String (Indexed)
    dtg: Date (Time-index)
    geom: Point (Geo-index)

Deleting a feature type
^^^^^^^^^^^^^^^^^^^^^^^

Continuing on, let's delete the first feature type we created with the
``remove-schema`` command. The ``remove-schema`` command takes two flags:

-  ``-c`` or ``--catalog``: the name of the catalog table
-  ``-f`` or ``--feature-name``: the name of the feature to delete

Run the following command:

.. code:: bash

    geomesa remove-schema -u <username> -p <password> -c cmd_tutorial -f feature

NOTE: Running this command will take a bit longer than the previous two,
as it will delete three tables in Accumulo, as well as remove the
metadata rows in the catalog table associated with the feature.

The output should resemble the following:

.. code:: bash

    Remove schema feature from catalog cmd_tutorial? (yes/no): yes
    Starting
    State change: CONNECTED
    Removed feature

Ingesting Data
--------------

GeoMesa Tools is a set of command line tools to add feature management
functions, query planning and explanation, ingest, and export abilities
from the command line. In this tutorial, we'll cover how to ingest and
export features using GeoMesa Tools.

Getting Data
^^^^^^^^^^^^

For this tutorial we will be using the GDELT data set, available here:
http://data.gdeltproject.org/events/index.html.  Download any daily data file,
for example::

   20160119.export.CSV.zip

and unzip the file on your computer.

.. note::

    The unpacked files have ``*.CSV`` extensions but the data within them are
    actually *tab* separated.

Ingesting Features
^^^^^^^^^^^^^^^^^^

The ingest command currently supports three formats: CSV, TSV, and SHP.

The ``ingest`` command has the following required flags:

-  ``-u`` or ``--user``: the Accumulo user
-  ``-c`` or ``--catalog``: the name of the GeoMesa catalog table
-  ``-f`` or ``--feature-name``: the name of the feature to ingest

One (not both) of the following flags must also be specified:

-  ``-p`` or ``--password``: the Accumulo password
-  ``--keytab``: path to a Kerberos keytab file

If ``-p`` (or ``--password``) and ``--keytab`` are both omitted, then password authentication is assumed and the user
is prompted for a password.

If ``$ACCUMULO_HOME`` does not contain the configuration of the Accumulo
instance you wish to connect to, you also must specify the connection
parameters for Accumulo:

-  ``-i`` or ``--instance``: the Accumulo instance
-  ``-z`` or ``--zookeepers``: a comma-separated list of Zookeeper hosts

The optional ``-C`` switch lets you specify a converter defined in a JSON-based
instruction file about how to convert the data as GeoMesa reads it. The
converter library handles many of the data transformations necessary to fit a
raw data set into a simple feature type suitable for use in GeoMesa
applications. Conversions can take advantage of a variety of features such as
``concatenate()`` and ``stringToInteger()`` functions as well as the use of regular
expressions. For more information see :ref:`setting_up_ingest_converter` below.

The last argument that is required for all ingest commands is the path
to the file to ingest. If ingesting CSV/TSV data this can be an HDFS
path, specified by prefixing it with ``hdfs://``.

.. _setting_up_ingest_converter:

Setting up an Ingest Converter
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To use the ``-C`` switch, create (or edit) the file
``$GEOMESA_ACCUMULO_HOME/conf/application.conf``, which serves as the converter
configuration file, to add the ``gdelt`` SimpleFeatureType and a converter
``gdelt_csv`` for reading the data from tab-separated value files:

.. code::

    geomesa {
      sfts {
        gdelt = {
          fields = [
            { name = globalEventId, type = String, index = false}
            { name = eventCode, type = String }
            { name = actor1, type = String }
            { name = actor2, type = String }
            { name = dtg, type = Date, index = true }
            { name = geom, type = Point, srid = 4326 }
          ]
        }
      }
      converters {
        gdelt_tsv = {
          type = delimited-text
          format = TDF
          id-field = "$1" // global event id
          fields = [
            { name = globalEventId, transform = "$1" }
            { name = eventCode,     transform = "$27" }
            { name = actor1,        transform = "$7" }
            { name = actor2,        transform = "$17" }
            { name = dtg,           transform = "date('yyyyMMdd', $2)" }
            { name = geom,          transform = "point(stringToDouble($41, 0.0), $40::double)" }
          ]
        }
      }
    }

The config file needs to have a ``SimpleFeatureType`` defined along with a
converter that specifies instructions on how to turn the raw data file into
that simple feature type. See :doc:`/user/convert/index` for a more details
on converters, including a full list of the transformation functions available
(:doc:`/user/convert/function_overview`).

This example uses the ``date()`` function to tell the parser what date column
is in. The ``stringToDouble()`` and ``::double`` functions give two different
methods for type casting. The ``stringTo<dataType>()`` methods take in the
value to be cast as well as a prespecified default that will be returned if
there is an exception, whereas the ``::double`` function will fail (and drop
the record) if the casting fails.

To confirm that GeoMesa can properly parse your edited
``$GEOMESA_ACCUMULO_HOME/conf/application.conf`` file, use ``geomesa-accumulo env``:

.. code::

    $ geomesa-accumulo env -s gdelt --format spec
    Using GEOMESA_ACCUMULO_HOME = /opt/geomesa/tools
    Simple Feature Types:
    gdelt = globalEventId:String,eventCode:String,actor1:String,actor2:String,dtg:Date:index=join,*geom:Point:srid=4326;geomesa.index.dtg='dtg'

    $ geomesa-accumulo env -c gdelt_tsv
    Using GEOMESA_ACCUMULO_HOME = /opt/geomesa/tools

    Simple Feature Type Converters:
    converter-name=gdelt_tsv
    fields=[
        {
            name=globalEventId
            transform="$1"
        },
        {
            name=eventCode
            transform="$27"
        },
        {
            name=actor1
            transform="$7"
        },
        {
            name=actor2
            transform="$17"
        },
        {
            name=dtg
            transform="date('yyyyMMdd', $2)"
        },
        {
            name=geom
            transform="point(stringToDouble($41, 0.0), $40::double)"
        }
    ]
    format=TDF
    # global event id
    id-field="$1"
    type=delimited-text


Downloading sample data 
^^^^^^^^^^^^^^^^^^^^^^^

Packaged with geomesa script for easily downloading publicly 
available data sets and a set of corresponding config files.

The currently available data sets are GDELT_, GeoLife_, OSM-GPX_, T-Drive_, GeoNames_, 
NYCTaxi_, GTD_, and Twitter_. The first five of these sets are easily downloadable via a provided script.

.. _GDELT: http://gdeltproject.org/
.. _GeoLife: http://research.microsoft.com/en-us/projects/geolife/
.. _OSM-GPX: http://wiki.openstreetmap.org/wiki/Planet.gpx
.. _T-Drive: http://research.microsoft.com/apps/pubs/?id=152883
.. _GeoNames: http://www.geonames.org/
.. _NYCTaxi: https://publish.illinois.edu/dbwork/open-data/
.. _GTD: http://www.start.umd.edu/gtd/
.. _Twitter: https://dev.twitter.com/rest/public

To download these sets, run the download script found in geomesa-tools/bin and 
provide the name of the data set desired. 
This can be one of ``gdelt``, ``geolife``, ``osm-gpx``, ``tdrive``, or ``geonames``:

Example Usage:

.. code:: bash

    $ ./download-data.sh geolife

Depending on the desired data, you may be prompted further information to specify desired dates or locations.

The resulting data will then be downloaded to ``$GEOMESA_ACCUMULO_HOME/data``.

Configuration files for these data sets are found under ``$GEOMESA_ACCUMULO_HOME/conf/sfts``.
Modifications to them can seen by running ``geomesa-accumulo env`` and will be reflected in the next run ingest.

Running an Ingest
^^^^^^^^^^^^^^^^^

Now that we have everything ready, we will now
combine the various parameters into the following complete ingest
command:

.. code-block:: bash

    $ geomesa-accumulo ingest \
       -u <username> -p <password> -i <instance> -z <zookeepers> \
       -c gdelt -s gdelt -C gdelt_tsv --threads 1 \
       /path/to/<gdelt-data-file>.csv

``<username>`` and ``<password>`` are the credentials associated with
the Accumulo instance. ``<instance>`` and ``<zookeepers>`` are the
connection parameters for Accumulo, if this is not specified in the
configuration files in ``$ACCUMULO_HOME``.

Exporting Features
------------------

Let's export your newly ingested features in a couple of file formats.
Currently, the ``export`` command supports exports to CSV, TSV,
Shapefile, GeoJSON, and GML. We'll do one of each format in this next
section.

The ``export`` command has 3 required flags:

-  ``-c`` or ``--catalog``: the name of the catalog table
-  ``-f`` or ``--feature-name``: the name of the feature to export
-  ``-F`` or ``--format``: the output format (``csv``, ``tsv``,
   ``shp``, ``geojson``, or ``gml``)

Additionally, you can specify more details about the kind of export you
would like to perform with optional flags for ``export``:

-  ``-a`` or ``--attributes``: the attributes of the feature to return
-  ``-m`` or ``--max-features``: the maximum number of features to
   return in an export
-  ``-q`` or ``--query``: a `CQL
   query <http://docs.geotools.org/latest/userguide/library/cql/index.html>`__
   to perform on the features, to return only subset of features
   matching the query

We'll use the ``--max-features`` flag to ensure our dataset is small and
quick to export. First, we'll export to CSV with the following command:

.. code-block:: bash

    $ geomesa-accumulo export -u <username> -p <password> -c gdelt_Ukraine -fn gdelt -fmt csv -max 50
    # or specifying Accumulo configuration explicitly:
    $ geomesa-accumulo export \
        -u <username> -p <password> -i <instance> -z <zookeepers> \
        -c gdelt -f gdelt -f csv -m 50

This command will output the relevant rows to the console. Inspect the
rows now, or pipe the output into a file for later review.

Now, run the above command four additional times, changing the
``--format`` flag to ``tsv``, ``shp``, ``json``, and ``gml``. The
``shp`` format also requires the ``-o`` option to specify the name of an
output file.
