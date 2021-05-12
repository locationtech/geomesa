Installing GeoMesa Kudu
=======================

.. note::

    GeoMesa currently supports Kudu version |kudu_version|.

Installing the Binary Distribution
----------------------------------

GeoMesa Kudu artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

.. note::

  In the following examples, replace ``${TAG}`` with the corresponding GeoMesa version (e.g. |release_version|), and
  ``${VERSION}`` with the appropriate Scala plus GeoMesa versions (e.g. |scala_release_version|).

Extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa-${TAG}/geomesa-kudu_${VERSION}-bin.tar.gz"
    $ tar xvf geomesa-kudu_${VERSION}-bin.tar.gz
    $ cd geomesa-kudu_${VERSION}
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
``geomesa-kudu_${VERSION}/bin/`` of the binary distribution.

Configuring the Classpath
^^^^^^^^^^^^^^^^^^^^^^^^^

GeoMesa needs Hadoop JARs on the classpath. These are not bundled by default, as they should match
the versions installed on the target system.

If the environment variable ``HADOOP_HOME`` is set, then GeoMesa will load the appropriate
JARs and configuration files from those locations and no further configuration is required. Otherwise, you will
be prompted to download the appropriate JARs the first time you invoke the tools. Environment variables can be
specified in ``conf/*-env.sh`` and dependency versions can be specified in ``conf/dependencies.sh``.

In order to run map/reduce jobs, the Hadoop ``*-site.xml`` configuration files from your Hadoop installation
must be on the classpath. If ``HADOOP_HOME`` is not set, then copy them into ``geomesa-kudu_${VERSION}/conf``.

GeoMesa also provides the ability to add additional JARs to the classpath using the environmental variable
``$GEOMESA_EXTRA_CLASSPATHS``. GeoMesa will prepend the contents of this environmental variable  to the computed
classpath, giving it highest precedence in the classpath. Users can provide directories of jar files or individual
files using a colon (``:``) as a delimiter. These entries will also be added the the map-reduce libjars variable.

Use the ``geomesa-kudu classpath`` command in order to see what JARs are being used.

Due to licensing restrictions, dependencies for shape file support must be separately installed.
Do this with the following command:

.. code-block:: bash

    $ ./bin/install-shapefile-support.sh

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

    See :ref:`geoserver_versions` to ensure that GeoServer is compatible with your GeoMesa version.

The Kudu GeoServer plugin is bundled by default in a GeoMesa binary distribution. To install, extract
``$GEOMESA_KUDU_HOME/dist/gs-plugins/geomesa-kudu-gs-plugin_${VERSION}-install.tar.gz`` into GeoServer's
``WEB-INF/lib`` directory.

Restart GeoServer after the JARs are installed.
