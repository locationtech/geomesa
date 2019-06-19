Installing GeoMesa Redis
========================

Installing the Binary Distribution
----------------------------------

GeoMesa Redis artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version
(|release|) from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

Extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-$VERSION/geomesa-redis_2.11-$VERSION-bin.tar.gz"
    $ tar xvf geomesa-redis_2.11-$VERSION-bin.tar.gz
    $ cd geomesa-redis_2.11-$VERSION
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt  logs/

.. _redis_install_source:

Building from Source
--------------------

GeoMesa Redis may also be built from source. For more information refer to :ref:`building_from_source`
in the developer manual, or to the ``README.md`` file in the the source distribution.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa Redis
distribution. If you have built from source, the distribution is created in the ``target`` directory of
``geomesa-redis/geomesa-redis-dist``.

More information about developing with GeoMesa may be found in the :doc:`/developer/index`.

.. _setting_up_redis_commandline:

Setting up the Redis Command Line Tools
---------------------------------------

GeoMesa comes with a set of command line tools for managing Redis features located in
``geomesa-redis_2.11-$VERSION/bin/`` of the binary distribution.

If desired, you may use the included script ``bin/geomesa-redis configure`` to help set up the environment variables
used by the tools. Otherwise, you may invoke the ``geomesa-redis`` script using the fully-qualified path, and
use the default configuration.

.. note::

    See :ref:`slf4j_configuration` for information about configuring the SLF4J implementation.

Test the command that invokes the GeoMesa Tools:

.. code::

    $ bin/geomesa-redis
    INFO  Usage: geomesa-redis [command] [command options]
      Commands:
      ...

For more details on the available commands, see :ref:`redis_tools`.

Due to licensing restrictions, dependencies for shape file support must be separately installed.
Install them with the following scripts:

.. code-block:: bash

    $ bin/install-jai.sh
    $ bin/install-jline.sh

Use the ``geomesa-redis classpath`` command in order to see what JARs are being used.

If the classpath needs to be modified, ``geomesa-redis`` will pull additional entries from the
``GEOMESA_EXTRA_CLASSPATHS`` environment variable, if it is defined.

Note that the ``GEOMESA_EXTRA_CLASSPATHS`` variable follows standard
`Java Classpath <http://docs.oracle.com/javase/8/docs/technotes/tools/windows/classpath.html>`_ conventions, which
generally means that entries must be directories, JAR, or zip files. Individual XML files will be ignored. For example,
to add a ``core-site.xml`` file to the classpath you must either include a directory on the
classpath or add the file to a zip or JAR archive to be included on the classpath.

.. _install_redis_geoserver:

Installing GeoMesa Redis in GeoServer
-------------------------------------

.. warning::

    GeoMesa 2.2.x and 2.3.x require GeoServer 2.14.x. GeoMesa 2.1.x and earlier require GeoServer 2.12.x.

The Redis GeoServer plugin is bundled by default in a GeoMesa binary distribution. To install, extract
``$GEOMESA_REDIS_HOME/dist/gs-plugins/geomesa-redis-gs-plugin_2.11-$VERSION-install.tar.gz`` into GeoServer's
``WEB-INF/lib`` directory.

Restart GeoServer after the JARs are installed. See :doc:`/user/redis/geoserver` for details on configuring stores
and layers.
