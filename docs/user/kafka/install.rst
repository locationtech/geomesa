Installing GeoMesa Kafka
========================

.. warning::

    Using the `Kafka <http://kafka.apache.org/>`_ module requires Kafka |kafka_version|
    and ZooKeeper |zookeeper_version|.

.. note::

    These instructions are identical for Kafka 0.8.x, 0.9.x, and 0.10.x.
    The value of ``$KAFKAVERSION`` is "08" for Kafka 0.8.x, "09" for Kafka 0.9.x,
    or "10" for Kafka 0.10.x.

Installing from the Binary Distribution
---------------------------------------

GeoMesa Kafka artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version
(``$VERSION`` = |release|) matching the version of Kafka you are using:

* Kafka 0.8.x release: |release_tarball_kafka08|
* Kafka 0.9.x release: |release_tarball_kafka09|
* Kafka 0.10.x release: |release_tarball_kafka10|

Extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution for Kafka 0.9:
    $ wget https://repo.locationtech.org/content/repositories/geomesa-releases/org/locationtech/geomesa/geomesa-kafka-$KAFKAVERSION-dist_2.11/$VERSION/geomesa-kafka-$KAFKAVERSION-dist_2.11-$VERSION-bin.tar.gz
    $ tar xzvf geomesa-kafka-$KAFKAVERSION-dist_2.11-$VERSION-bin.tar.gz
    $ cd geomesa-kafka-$KAFKAVERSION-dist_2.11-$VERSION
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt

.. _kafka_install_source:

Building from Source
--------------------

GeoMesa Kafka may also be built from source. For more information refer to :ref:`building_from_source`
in the developer manual, or to the ``README.md`` file in the the source distribution.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa distribution.

If you have built from source, the Kafka distributions are created in the
respective ``target`` directory of each module below:

 * Kafka 0.8.x: ``geomesa-kafka/geomesa-kafka-dist/geomesa-kafka-08-dist``
 * Kafka 0.9.x: ``geomesa-kafka/geomesa-kafka-dist/geomesa-kafka-09-dist``
 * Kafka 0.10.x: ``geomesa-kafka/geomesa-kafka-dist/geomesa-kafka-10-dist``

More information about developing with GeoMesa may be found in the :doc:`/developer/index`.

.. _setting_up_kafka_commandline:

Setting up the Kafka Command Line Tools
---------------------------------------

GeoMesa comes with a set of command line tools for managing Kafka features. In each Kafka distribution the
tools are located in ``geomesa-kafka-$KAFKAVERSION-dist_2.11-$VERSION-bin.tar.gz/bin/``.

.. note::

    You can configure environment variables and classpath settings in
    ``geomesa-kafka-$KAFKAVERSION_2.11-$VERSION/bin/geomesa-env.sh``

In the ``geomesa-kafka-$KAFKAVERSION_2.11-$VERSION`` directory, run ``bin/geomesa-kafka configure``
to set up the tools.

.. code-block:: bash

    ### in geomesa-kafka-$KAFKAVERSION_2.11-$VERSION:
    $ bin/geomesa-kafka configure
    Using GEOMESA_KAFKA_HOME as set: /path/to/geomesa-kafka-$KAFKAVERSION_2.11-$VERSION
    Is this intentional? Y\n y
    Warning: GEOMESA_LIB already set, probably by a prior configuration.
    Current value is /path/to/geomesa-kafka-$KAFKAVERSION_2.11-$VERSION/lib.

    Is this intentional? Y\n y

    To persist the configuration please update your bashrc file to include:
    export GEOMESA_KAFKA_HOME=/path/to/geomesa-kafka-$KAFKAVERSION_2.11-$VERSION
    export PATH=${GEOMESA_KAFKA_HOME}/bin:$PATH

Update and re-source your ``~/.bashrc`` file to include the ``$GEOMESA_KAFKA_HOME`` and ``$PATH`` updates.

.. warning::

    Please note that the ``$GEOMESA_KAFKA_HOME`` variable points to the location of the ``geomesa-kafka-$KAFKAVERSION_2.11-$VERSION``
    directory, not the main geomesa binary distribution directory.

.. note::

    ``geomesa-kafka`` will read the ``$GEOMESA_EXTRA_CLASSPATHS`` environment variable to include any
    additional jars or directories in the classpath. Use the ``geomesa-kafka classpath`` command in order to see what
    JARs are being used.

Test the command that invokes the GeoMesa Tools:

.. code::

    $ geomesa-kafka
    Using GEOMESA_KAFKA_HOME = /path/to/geomesa-kafka-$KAFKAVERSION_2.11-$VERSION
    Usage: geomesa-kafka [command] [command options]
      Commands:
        convert         Convert files using GeoMesa's internal SFT converter framework
        create-schema   Create a feature definition in GeoMesa
        get-schema      Describe the attributes of a given feature in GeoMesa
        get-names       List GeoMesa features for a given zkPath
        help            Show help
        listen          Listen to a GeoMesa Kafka topic
        remove-schema   Remove a schema and associated features from GeoMesa
        version         GeoMesa Version

.. _install_kafka_geoserver:

Installing GeoMesa Kafka in GeoServer
-------------------------------------

.. warning::

    The GeoMesa Kafka GeoServer plugin requires the use of GeoServer
    |geoserver_version| and GeoTools |geotools_version|.

As described in section :ref:`geomesa_and_geoserver`, GeoMesa implements a
`GeoTools`_-compatible data store. This makes it possible
to use GeoMesa Keafka as a data store in `GeoServer`_.
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
    0 | geomesa-kafka-$KAFKAVERSION-gs-plugin_2.11-$VERSION

    Module(s) to install: 0
    0 | Installing geomesa-kafka-$KAFKAVERSION-gs-plugin_2.11-$VERSION-install.tar.gz
    Done

Alternatively, extract the contents of the appropriate plugin archive into the GeoServer
``WEB-INF/lib`` directory. If you are using Tomcat:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-kafka-$KAFKAVERSION-gs-plugin/dist/gs-plugins/geomesa-kafka-$KAFKAVERSION-gs-plugin_2.11-$VERSION-install.tar.gz \
      -C /path/to/tomcat/webapps/geoserver/WEB-INF/lib/

If you are using GeoServer's built in Jetty web server:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-kafka-$KAFKAVERSION-gs-plugin/dist/gs-plugins/geomesa-kafka-$KAFKAVERSION-gs-plugin_2.11-$VERSION-install.tar.gz \
      -C /path/to/geoserver/webapps/geoserver/WEB-INF/lib/

This will install the JARs for the Kafka GeoServer plugin and most of its dependencies.
However, you will also need additional JARs for Kafka and Zookeeper that will
be specific to your installation that you will also need to copy to GeoServer's
``WEB-INF/lib`` directory. For example,you should use the JARs that match the version of
Kafka you are running.

.. warning::

    GeoMesa |release| depends on Scala 2.11, so you should make sure you use the
    Kafka version built with Scala 2.11 as well (``kafka_2.11_*``) to avoid
    compatibility issues.

Copy these additional dependencies (or the equivalents for your Kafka installation) to
your GeoServer ``WEB-INF/lib`` directory.

Kafka 0.8

    * kafka-clients-0.8.2.1.jar
    * kafka_2.11-0.8.2.1.jar
    * metrics-core-2.2.0.jar
    * zkclient-0.3.jar
    * zookeeper-3.4.6.jar

Kafka 0.9

    * kafka-clients-0.9.0.1.jar
    * kafka_2.11-0.9.0.1.jar
    * metrics-core-2.2.0.jar
    * zkclient-0.7.jar
    * zookeeper-3.4.6.jar

Kafka 0.10

    * kafka-clients-0.10.0.1.jar
    * kafka-2.11-0.10.0.1.jar
    * metrics-core-2.2.0.jar
    * zkclient-0.8.jar
    * zookeeper-3.4.6.jar

There is a script in the ``geomesa-kafka-$KAFKAVERSION_2.11-$VERSION/bin`` directory
(``$GEOMESA_KAFKA_HOME/bin/install-kafka.sh``) which will install these
dependencies to a target directory using ``wget`` (requires an internet
connection).

Restart GeoServer after the JARs are installed.
