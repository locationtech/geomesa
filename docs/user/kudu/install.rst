Installing GeoMesa Kudu
=======================

.. note::

    GeoMesa currently supports Kudu version |kudu_version|.

Installing the Binary Distribution
----------------------------------

GeoMesa Kudu artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version
(|release|) from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

Extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-$VERSION/geomesa-kudu_2.11-$VERSION-bin.tar.gz"
    $ tar xvf geomesa-kudu_2.11-$VERSION-bin.tar.gz
    $ cd geomesa-kudu_2.11-$VERSION
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt  logs/

.. _kudu_install_source:

Building from Source
--------------------

GeoMesa Kudu may also be built from source. For more information refer to :ref:`building_from_source`
in the developer manual, or to the ``README.md`` file in the the source distribution.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa Kudu
distribution. If you have built from source, the distribution is created in the ``target`` directory of
``geomesa-kudu/geomesa-kudu-dist``.

More information about developing with GeoMesa may be found in the :doc:`/developer/index`.

.. _setting_up_kudu_commandline:

Setting up the Kudu Command Line Tools
--------------------------------------

GeoMesa comes with a set of command line tools for managing Kudu features located in
``geomesa-kudu_2.11-$VERSION/bin/`` of the binary distribution.

In order to run distributed ingest from the command-line tools, Hadoop must be available on the classpath.
By default, GeoMesa will attempt to read Hadoop related environment variables to build the classpath. You can
configure environment variables and classpath settings in
``geomesa-kudu_2.11-$VERSION/conf/geomesa-env.sh`` or in your external env (e.g. bashrc file). The logic GeoMesa
uses to determine which external entries to include on the classpath is:

    1. If the environmental variable ``GEOMESA_HADOOP_CLASSPATH`` is set then GeoMesa
    will use it as the classpath and skip all other logic.

    2. Next, if ``$HADOOP_HOME`` is set then GeoMesa will attempt to build the classpath by
    searching for JAR and configuration files in standard locations. Note that this is very specific to the
    installation or distribution of Hadoop you are using and may not be reliable.

    3. If no environmental variables are set but the ``hadoop`` command is available then GeoMesa will
    generate the classpath by running ``hadoop classpath``. This method of classpath determination may be slow,
    so it is recommended that you manually set these variables in your environment or the
    ``conf/geomesa-env.sh`` file.

If installing on a system without Hadoop, the ``install-hadoop.sh`` scripts in the ``bin`` directory may be used to
download the required Hadoop JARs into the ``lib`` directory. You should edit this script to match the versions
used by your installation.

In addition, ``geomesa-kudu`` will pull any additional entries from the ``GEOMESA_EXTRA_CLASSPATHS``
environment variable.

Note that the ``GEOMESA_EXTRA_CLASSPATHS`` and ``GEOMESA_HADOOP_CLASSPATH`` variables both follow standard
`Java Classpath <http://docs.oracle.com/javase/8/docs/technotes/tools/windows/classpath.html>`_ conventions, which
generally means that entries must be directories, JAR, or zip files. Individual XML files will be ignored. For example,
to add a ``core-site.xml`` file to the classpath you must either include a directory on the
classpath or add the file to a zip or JAR archive to be included on the classpath.

Use the ``geomesa-kudu classpath`` command in order to see what JARs are being used.

Due to licensing restrictions, dependencies for shape file support must be separately installed.
Install them with the following scripts:

.. code-block:: bash

    $ bin/install-jai.sh
    $ bin/install-jline.sh

If desired, you may use the included script ``bin/geomesa-kudu configure`` to help set up the environment variables
used by the tools. Otherwise, you may invoke the ``geomesa-kudu`` script using the fully-qualified path, and
use the default configuration.

.. note::

    See :ref:`slf4j_configuration` for information about configuring the SLF4J implementation.

Test the command that invokes the GeoMesa Tools:

.. code::

    $ bin/geomesa-kudu
    INFO  Usage: geomesa-kudu [command] [command options]
      Commands:
      ...

For more details, see :ref:`kudu_tools`.

.. _install_kudu_geoserver:

Installing GeoMesa Kudu in GeoServer
------------------------------------

.. warning::

    GeoMesa 2.2.x and 2.3.x require GeoServer 2.14.x. GeoMesa 2.1.x and earlier require GeoServer 2.12.x.

The Kudu GeoServer plugin is bundled by default in a GeoMesa binary distribution. To install, extract
``$GEOMESA_KUDU_HOME/dist/gs-plugins/geomesa-kudu-gs-plugin_2.11-$VERSION-install.tar.gz`` into GeoServer's
``WEB-INF/lib`` directory.

Restart GeoServer after the JARs are installed.
