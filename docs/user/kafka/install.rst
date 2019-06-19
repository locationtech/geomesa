Installing GeoMesa Kafka
========================

.. note::

    GeoMesa currently supports Kafka version |kafka_version|. However, not all features are supported
    for versions prior to 1.0.

Installing from the Binary Distribution
---------------------------------------

GeoMesa Kafka artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version
(|release|) from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

Extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-$VERSION/geomesa-kafka_2.11-$VERSION-bin.tar.gz"
    $ tar xzvf geomesa-kafka_2.11-$VERSION-bin.tar.gz
    $ cd geomesa-kafka_2.11-$VERSION
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt

.. _kafka_install_source:

Building from Source
--------------------

GeoMesa Kafka may also be built from source. For more information refer to :ref:`building_from_source`
in the developer manual, or to the ``README.md`` file in the the source distribution.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa distribution.

If you have built from source, the Kafka distribution is created in the
``target`` directory of the ``geomesa-kafka/geomesa-kafka-dist`` module.

More information about developing with GeoMesa may be found in the :doc:`/developer/index`.

.. _setting_up_kafka_commandline:

Setting up the Kafka Command Line Tools
---------------------------------------

GeoMesa comes with a set of command line tools for managing Kafka features. In the Kafka distribution the
tools are located in ``geomesa-kafka_2.11-$VERSION/bin/``.

.. note::

    You can configure environment variables and classpath settings in
    ``geomesa-kafka_2.11-$VERSION/bin/geomesa-env.sh``

In the ``geomesa-kafka_2.11-$VERSION`` directory, run ``bin/geomesa-kafka configure``
to set up the tools.

.. code-block:: bash

    ### in geomesa-kafka_2.11-$VERSION:
    $ bin/geomesa-kafka configure
    Using GEOMESA_KAFKA_HOME as set: /path/to/geomesa-kafka_2.11-$VERSION
    Is this intentional? Y\n y
    Current value is /path/to/geomesa-kafka_2.11-$VERSION/lib.

    Is this intentional? Y\n y

    To persist the configuration please update your bashrc file to include:
    export GEOMESA_KAFKA_HOME=/path/to/geomesa-kafka_2.11-$VERSION
    export PATH=${GEOMESA_KAFKA_HOME}/bin:$PATH

Update and re-source your ``~/.bashrc`` file to include the ``$GEOMESA_KAFKA_HOME`` and ``$PATH`` updates.

.. warning::

    Please note that the ``$GEOMESA_KAFKA_HOME`` variable points to the location of the ``geomesa-kafka_2.11-$VERSION``
    directory, not the main geomesa binary distribution directory.


.. note::

    ``geomesa-kafka`` will read the ``$KAKFA_HOME`` and ``$ZOOKEEPER_HOME`` environment variables to load the
    appropriate Kafka JAR files. Alternatively, the ``install-kafka.sh`` script in the ``bin`` directory
    may be used to download the required JARs into the ``lib`` directory. You should edit this script to
    match the versions used by your installation.

    GeoMesa provides the ability to provide additional jars on the classpath using the environmental variable
    ``$GEOMESA_EXTRA_CLASSPATHS``. GeoMesa will prepend the contents of this environmental variable  to the computed
    classpath giving it highest precedence in the classpath. Users can provide directories of jar files or individual
    files using a colon (``:``) as a delimiter. Use the ``geomesa-kafka classpath`` command to print the final
    classpath that will be used when executing geomesa commands.

Test the command that invokes the GeoMesa Tools:

.. code::

    $ geomesa-kafka
    Usage: geomesa-kafka [command] [command options]
      Commands:
        ...

.. _install_kafka_geoserver:

Installing GeoMesa Kafka in GeoServer
-------------------------------------

.. warning::

    GeoMesa 2.2.x and 2.3.x require GeoServer 2.14.x. GeoMesa 2.1.x and earlier require GeoServer 2.12.x.

As described in section :ref:`geomesa_and_geoserver`, GeoMesa implements a
`GeoTools`_-compatible data store. This makes it possible
to use GeoMesa Kafka as a data store in `GeoServer`_.
GeoServer's web site includes `installation instructions for GeoServer`_.

.. _installation instructions for GeoServer: http://docs.geoserver.org/stable/en/user/installation/index.html

After GeoServer is running, you will also need to install the WPS plugin to
your GeoServer instance. The GeoServer WPS Plugin must match the version of
GeoServer instance. The GeoServer website includes instructions for downloading
and installing `the WPS plugin`_.

.. _the WPS plugin: http://docs.geoserver.org/stable/en/user/services/wps/install.html

.. note::

    If using Tomcat as a web server, it will most likely be necessary to
    pass some custom options::

        export CATALINA_OPTS="-Xmx8g -XX:MaxPermSize=512M -Duser.timezone=UTC \
        -server -Djava.awt.headless=true"

    The value of ``-Xmx`` should be as large as your system will permit; this
    is especially important for the Kafka plugin. You
    should also consider passing ``-DGEOWEBCACHE_CACHE_DIR=/tmp/$USER-gwc``
    and ``-DEPSG-HSQL.directory=/tmp/$USER-hsql``
    as well. Be sure to restart Tomcat for changes to take place.

To install GeoMesa's GeoServer plugin we can use the script ``manage-geoserver-plugins.sh`` in ``bin`` directory
of the appropriate GeoMesa Kafka binary distribution (see :ref:`versions_and_downloads`).

.. note::

    If $GEOSERVER_HOME is set, then the ``--lib-dir`` parameter is not needed.

.. code-block:: bash

    $ bin/manage-geoserver-plugins.sh --lib-dir /path/to/geoserver/WEB-INF/lib/ --install
    Collecting Installed Jars
    Collecting geomesa-gs-plugin Jars

    Please choose which modules to install
    Multiple may be specified, eg: 1 4 10
    Type 'a' to specify all
    --------------------------------------
    0 | geomesa-kafka-gs-plugin_2.11-$VERSION

    Module(s) to install: 0
    0 | Installing geomesa-kafka-gs-plugin_2.11-$VERSION-install.tar.gz
    Done

Alternatively, extract the contents of the appropriate plugin archive into the GeoServer
``WEB-INF/lib`` directory. If you are using Tomcat:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-kafka-gs-plugin/dist/gs-plugins/geomesa-kafka-gs-plugin_2.11-$VERSION-install.tar.gz \
      -C /path/to/tomcat/webapps/geoserver/WEB-INF/lib/

If you are using GeoServer's built in Jetty web server:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-kafka-gs-plugin/dist/gs-plugins/geomesa-kafka-gs-plugin_2.11-$VERSION-install.tar.gz \
      -C /path/to/geoserver/webapps/geoserver/WEB-INF/lib/

This will install the JARs for the Kafka GeoServer plugin and most of its dependencies.
However, you will also need additional JARs for Kafka and Zookeeper that will
be specific to your installation.

.. warning::

    GeoMesa |release| depends on Scala 2.11, so you should make sure you use the
    Kafka version built with Scala 2.11 as well (``kafka_2.11_*``) to avoid
    compatibility issues.

Copy these additional dependencies (or the equivalents for your Kafka installation) to
your GeoServer ``WEB-INF/lib`` directory:

.. tabs::

    .. tab:: Kafka 2.0.0

        * kafka-clients-2.0.0.jar
        * kafka_2.11-2.0.0.jar
        * zkclient-0.10.jar
        * zookeeper-3.4.10.jar
        * metrics-core-2.2.0.jar
        * jopt-simple-5.0.4.jar

    .. tab:: Kafka 1.0.1

        * kafka-clients-1.0.1.jar
        * kafka_2.11-1.0.1.jar
        * zkclient-0.10.jar
        * zookeeper-3.4.10.jar
        * metrics-core-2.2.0.jar
        * jopt-simple-5.0.4.jar

    .. tab:: Kafka 0.10

        * kafka-clients-0.10.2.1.jar
        * kafka-2.11-0.10.2.1.jar
        * zkclient-0.10.jar
        * zookeeper-3.4.10.jar
        * metrics-core-2.2.0.jar

    .. tab:: Kafka 0.9

        * kafka-clients-0.9.0.1.jar
        * kafka_2.11-0.9.0.1.jar
        * zkclient-0.7.jar
        * zookeeper-3.4.10.jar
        * metrics-core-2.2.0.jar

There is a script in the ``geomesa-kafka_2.11-$VERSION/bin`` directory
(``$GEOMESA_KAFKA_HOME/bin/install-kafka.sh``) which will install these
dependencies to a target directory using ``curl`` (requires an internet
connection). Edit the script before running to ensure the correct JAR versions
are specified.

Restart GeoServer after the JARs are installed.
