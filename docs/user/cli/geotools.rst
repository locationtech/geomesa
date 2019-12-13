.. _gt_tools:

GeoTools Command-Line Tools
===========================

The GeoMesa GeoTools distribution includes a set of command-line tools that can work with most non-GeoMesa
data store implementations to provide feature management, ingest and export. This allows the use of GeoMesa
converters and output encodings to be used with non-GeoMesa data stores.

Installation
------------

GeoMesa GeoTools artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version
(|release|) from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

Extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-$VERSION/geomesa-gt_2.11-$VERSION-bin.tar.gz"
    $ tar xvf geomesa-gt_2.11-$VERSION-bin.tar.gz
    $ cd geomesa-gt_2.11-$VERSION
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt  logs/

Alternatively, it may be built from source. For more information refer to :ref:`building_from_source`
in the developer manual, or to the ``README.md`` file in the the source distribution. If you have built from
source, the distribution is created in the ``target`` directory of ``geomesa-gt/geomesa-gt-dist``.

Setting up the Command Line Tools
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once installed, the command line tools can be invoked by running the script ``geomesa-gt`` located in the binary
distribution under ``geomesa-gt_2.11-$VERSION/bin/``.

The tools ship with some default GeoTools data stores, such as support for Postgis and shapefiles. For other stores,
you will need to copy the appropriate JARs into the tools ``lib`` folder.

.. note::

    You can configure environment variables and classpath settings in ``geomesa-gt_2.11-$VERSION/conf/geomesa-env.sh``.

.. note::

    ``geomesa-gt`` will load JARs from the ``$GEOMESA_EXTRA_CLASSPATHS`` environment variable
    into the class path. Use the ``geomesa-gt classpath`` command in order to see what JARs are being used.


Due to licensing restrictions, dependencies for shapefile support must be separately installed. Do this with the
following commands:

.. code-block:: bash

    $ bin/install-jai.sh
    $ bin/install-jline.sh

Run ``geomesa-gt`` without arguments to confirm that the tools work.

.. code::

    $ bin/geomesa-gt
    INFO  Usage: geomesa-gt [command] [command options]
      Commands:
      ...

Setting up Distributed Processing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

GeoMesa supports running map/reduce jobs for ingest and export. If you have a local Hadoop installation,
the tools will read the ``$HADOOP_HOME`` environment variable to load the appropriate JAR files for Hadoop.

If you do not have a local Hadoop installation, then in order to run distributed jobs you will need to manually
install the Hadoop configuration files into the tools ``conf`` folder, and the Hadoop JARs into the tools ``lib``
folder. To install JARs, use the script provided with the distribution:

.. code-block:: bash

    $ bin/install-hadoop.sh lib

If you installed JARs for any additional data stores, you will need to add them to the Hadoop libjars path
by modifying the file ``org/locationtech/geomesa/geotools/tools/gt-libjars.list`` inside the JAR
``lib/geomesa-gt-tools_2.11-$VERSION.jar``.

General Arguments
-----------------

Most commands require you to specify the connection to your data store. Parameters can be passed in using the
``--param`` argument, which can be repeated in order to specify multiple parameters. Alternatively, the ``--params``
argument can be used to specify a Java properties file containing the parameters. This may be useful for
simplifying the command invocation, or to hide sensitive parameters from bash history and process lists. If both
``--param`` and ``--params`` are used, then parameters specified directly will take precedence over ones from the
properties file.

For example, to connect to a Postgis data store, you may use the following command:

.. code::

    $ bin/geomesa-gt export --param dbtype=postgis --param host=localhost \
      --param user=postgres --param passwd=postgres --param port=5432 \
      --param database=example --feature-name gdelt
