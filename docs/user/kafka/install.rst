Installing GeoMesa Kafka
========================

.. note::

    GeoMesa currently supports Kafka |kafka_supported_versions|.

.. note::

    The examples below expect a version to be set in the environment:

    .. parsed-literal::

        $ export TAG="|release_version|"
        $ export VERSION="|scala_binary_version|-${TAG}" # note: |scala_binary_version| is the Scala build version

Installing from the Binary Distribution
---------------------------------------

GeoMesa Kafka artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

<<<<<<< HEAD
Download and extract it somewhere convenient:
=======
.. note::

  In the following examples, replace ``${TAG}`` with the corresponding GeoMesa version (e.g. |release_version|), and
  ``${VERSION}`` with the appropriate Scala plus GeoMesa versions (e.g. |scala_release_version|).

Extract it somewhere convenient:
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa-${TAG}/geomesa-kafka_${VERSION}-bin.tar.gz"
<<<<<<< HEAD
    $ tar xvf geomesa-kafka_${VERSION}-bin.tar.gz
    $ cd geomesa-kafka_${VERSION}
=======
    $ tar xzvf geomesa-kafka_${VERSION}-bin.tar.gz
    $ cd geomesa-kafka_${VERSION}
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

.. _kafka_install_source:

Building from Source
--------------------

GeoMesa Kafka may also be built from source. For more information, refer to the instructions on
`GitHub <https://github.com/locationtech/geomesa#building-from-source>`__.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa distribution.

If you have built from source, the Kafka distribution is created in the
``target`` directory of the ``geomesa-kafka/geomesa-kafka-dist`` module.

.. _setting_up_kafka_commandline:

Setting up the Kafka Command Line Tools
---------------------------------------

GeoMesa comes with a set of command line tools for managing Kafka features. In the Kafka distribution the
tools are located in ``geomesa-kafka_${VERSION}/bin/``.

If the environment variables ``KAFKA_HOME`` and ``ZOOKEEPER_HOME`` are set, then GeoMesa will load the appropriate
JARs and configuration files from those locations and no further configuration is required. Otherwise, you will
be prompted to download the appropriate JARs the first time you invoke the tools. Environment variables can be
specified in ``conf/*-env.sh`` and dependency versions can be specified in ``conf/dependencies.sh``.

GeoMesa also provides the ability to add additional JARs to the classpath using the environmental variable
``$GEOMESA_EXTRA_CLASSPATHS``. GeoMesa will prepend the contents of this environmental variable  to the computed
classpath, giving it highest precedence in the classpath. Users can provide directories of jar files or individual
files using a colon (``:``) as a delimiter. These entries will also be added the the map-reduce libjars variable.

Due to licensing restrictions, dependencies for shape file support must be separately installed.
Do this with the following command:

.. code-block:: bash

    $ ./bin/install-shapefile-support.sh

If working with Parquet files, install the required dependencies with the following command:

.. code-block:: bash

    $ ./bin/install-parquet-support.sh

Test the command that invokes the GeoMesa Tools:

.. code-block:: bash

    $ geomesa-kafka

The output should look like this::

    Usage: geomesa-kafka [command] [command options]
      Commands:
        ...

.. _install_kafka_geoserver:

Installing GeoMesa Kafka in GeoServer
-------------------------------------

.. warning::

    See :ref:`geoserver_versions` to ensure that GeoServer is compatible with your GeoMesa version.

Installing GeoServer
^^^^^^^^^^^^^^^^^^^^

As described in section :ref:`geomesa_and_geoserver`, GeoMesa implements a
`GeoTools`_-compatible data store. This makes it possible
to use GeoMesa Kafka as a data store in `GeoServer`_.
GeoServer's web site includes `installation instructions for GeoServer`_.

.. _installation instructions for GeoServer: https://docs.geoserver.org/stable/en/user/installation/index.html

After GeoServer is running, you will also need to install the WPS plugin to
your GeoServer instance. The GeoServer WPS Plugin must match the version of
GeoServer instance. The GeoServer website includes instructions for downloading
and installing `the WPS plugin`_.

.. _the WPS plugin: https://docs.geoserver.org/stable/en/user/services/wps/install.html

.. note::

    If using Tomcat as a web server, it will most likely be necessary to
    pass some custom options::

        export CATALINA_OPTS="-Xmx8g -XX:MaxPermSize=512M -Duser.timezone=UTC \
        -server -Djava.awt.headless=true"

    The value of ``-Xmx`` should be as large as your system will permit; this
    is especially important for the Kafka plugin. Be sure to restart Tomcat for changes to take place.

Installing the GeoMesa Kafka Data Store
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To install the GeoMesa data store, extract the contents of the
<<<<<<< HEAD
``geomesa-kafka-gs-plugin_${VERSION}-install.tar.gz`` file in ``geomesa-kafka_${VERSION}/dist/gs-plugins/``
=======
``geomesa-kafka-gs-plugin_${VERSION}-install.tar.gz`` file in ``geomesa-kafka_${VERSION}/dist/geoserver/``
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
in the binary distribution or ``geomesa-kafka/geomesa-kafka-gs-plugin/target/`` in the source
distribution into your GeoServer's ``lib`` directory:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-kafka_${VERSION}/dist/gs-plugins/geomesa-kafka-gs-plugin_${VERSION}-install.tar.gz \
      -C /path/to/geoserver/webapps/geoserver/WEB-INF/lib

Next, install the JARs for Kafka and Zookeeper. By default, JARs will be downloaded from Maven central. You may
override this by setting the environment variable ``GEOMESA_MAVEN_URL``. If you do not have an internet connection
you can download the JARs manually.

Edit the file ``geomesa-kafka_${VERSION}/conf/dependencies.sh`` to set the versions of Kafka and Zookeeper
to match the target environment, and then run the script:

.. code-block:: bash

    $ ./bin/install-dependencies.sh /path/to/geoserver/webapps/geoserver/WEB-INF/lib

.. warning::

<<<<<<< HEAD
    Ensure that the Scala version of both GeoMesa and Kafka match to avoid compatibility errors.
=======
    Ensure that the Scala version (either ``_2.11`` or ``_2.12``) of both GeoMesa and Kafka match to avoid
    compatibility issues.

The specific JARs needed for some common configurations are listed below:

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
        * kafka_2.11-0.10.2.1.jar
        * zkclient-0.10.jar
        * zookeeper-3.4.10.jar
        * metrics-core-2.2.0.jar
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

Restart GeoServer after the JARs are installed.
