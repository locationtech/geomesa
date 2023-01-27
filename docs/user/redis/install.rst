Installing GeoMesa Redis
========================

.. note::

    GeoMesa currently supports Redis |redis_supported_versions|.

.. note::

    The examples below expect a version to be set in the environment:

    .. parsed-literal::

        $ export TAG="|release_version|"
        $ export VERSION="|scala_binary_version|-${TAG}" # note: |scala_binary_version| is the Scala build version

Installing the Binary Distribution
----------------------------------

GeoMesa Redis artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

Download and extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa-${TAG}/geomesa-redis_${VERSION}-bin.tar.gz"
    $ tar xvf geomesa-redis_${VERSION}-bin.tar.gz
    $ cd geomesa-redis_${VERSION}

.. _redis_install_source:

Building from Source
--------------------

GeoMesa Redis may also be built from source. For more information, refer to the instructions on
`GitHub <https://github.com/locationtech/geomesa#building-from-source>`__.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa Redis
distribution. If you have built from source, the distribution is created in the ``target`` directory of
``geomesa-redis/geomesa-redis-dist``.

.. _setting_up_redis_commandline:

Setting up the Redis Command Line Tools
---------------------------------------

GeoMesa comes with a set of command line tools for managing Redis features located in
``geomesa-redis_${VERSION}/bin/`` of the binary distribution.

.. note::

    See :ref:`slf4j_configuration` for information about configuring the SLF4J implementation.

Test the command that invokes the GeoMesa Tools:

.. code-block:: bash

    $ ./bin/geomesa-redis

The output should look like this::

    INFO  Usage: geomesa-redis [command] [command options]
      Commands:
      ...

For more details on the available commands, see :ref:`redis_tools`.

Due to licensing restrictions, dependencies for shape file support must be separately installed.
Do this with the following command:

.. code-block:: bash

    $ ./bin/install-shapefile-support.sh

Use the ``geomesa-redis classpath`` command in order to see what JARs are being used.

If the classpath needs to be modified, ``geomesa-redis`` will pull additional entries from the
``GEOMESA_EXTRA_CLASSPATHS`` environment variable, if it is defined.

Note that the ``GEOMESA_EXTRA_CLASSPATHS`` variable follows standard
`Java Classpath <https://docs.oracle.com/javase/8/docs/technotes/tools/windows/classpath.html>`_ conventions, which
generally means that entries must be directories, JAR, or zip files. Individual XML files will be ignored. For example,
to add a ``core-site.xml`` file to the classpath you must either include a directory on the
classpath or add the file to a zip or JAR archive to be included on the classpath.

.. _install_redis_geoserver:

Installing GeoMesa Redis in GeoServer
-------------------------------------

.. warning::

    See :ref:`geoserver_versions` to ensure that GeoServer is compatible with your GeoMesa version.

The Redis GeoServer plugin is bundled by default in a GeoMesa binary distribution. To install, extract
``$GEOMESA_REDIS_HOME/dist/gs-plugins/geomesa-redis-gs-plugin_${VERSION}-install.tar.gz`` into GeoServer's
``WEB-INF/lib`` directory.

Restart GeoServer after the JARs are installed. See :doc:`/user/redis/geoserver` for details on configuring stores
and layers.
