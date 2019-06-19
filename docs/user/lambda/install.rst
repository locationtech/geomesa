Installing GeoMesa Lambda
=========================

Installing from the Binary Distribution
---------------------------------------

GeoMesa Lambda artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version
(|release|) from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

Extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-$VERSION/geomesa-lambda-dist_2.11-$VERSION-bin.tar.gz"
    $ tar xvf geomesa-lambda-dist_2.11-$VERSION-bin.tar.gz
    $ cd geomesa-lambda-dist_2.11-$VERSION
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt  logs/

.. _lambda_install_source:

Building from Source
--------------------

GeoMesa Lambda may also be built from source. For more information refer to :ref:`building_from_source`
in the developer manual, or to the ``README.md`` file in the the source distribution.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa Lambda
distribution. If you have built from source, the distribution is created in the ``target`` directory of
``geomesa-lambda/geomesa-lambda-dist``.

More information about developing with GeoMesa may be found in the :doc:`/developer/index`.

.. _install_lambda_runtime:

Installing the Accumulo Distributed Runtime Library
---------------------------------------------------

The Lambda data store requires the Accumulo data store distributed runtime to be installed. See
:ref:`install_accumulo_runtime`.

.. _setting_up_lambda_commandline:

Setting up the Lambda Command Line Tools
----------------------------------------

GeoMesa comes with a set of command line tools located in ``geomesa-lambda_2.11-$VERSION/bin/`` of the binary distribution.

.. note::

    You can configure environment variables and classpath settings in geomesa-lambda_2.11-$VERSION/conf/geomesa-env.sh.

In the ``geomesa-lambda_2.11-$VERSION`` directory, run ``bin/geomesa-lambda configure`` to set up the tools.

.. warning::

    Please note that the ``$GEOMESA_LAMBDA_HOME`` variable points to the location of the ``geomesa-lambda_2.11-$VERSION``
    directory, not the main geomesa binary distribution directory.

.. note::

    ``geomesa-lambda`` will read the ``$ACCUMULO_HOME``, ``$HADOOP_HOME`` and ``$KAFKA_HOME`` environment variables
    to load the runtime dependencies. If possible, we recommend installing the tools on the Accumulo master server,
    as you may also need various configuration files from Hadoop/Accumulo in order to run certain commands.

    GeoMesa provides the ability to provide additional jars on the classpath using the environmental variable
    ``$GEOMESA_EXTRA_CLASSPATHS``. GeoMesa will prepend the contents of this environmental variable  to the computed
    classpath giving it highest precedence in the classpath. Users can provide directories of jar files or individual
    files using a colon (``:``) as a delimiter. These entries will also be added the the mapreduce libjars variable.
    Use the ``geomesa classpath`` command to print the final classpath that will be used when executing geomesa
    commands.

    If you are running the tools on a system without Accumulo, Hadoop, or Kafka, the ``install-hadoop-accumulo.sh``
    and ``install-kafka.sh`` scripts in the ``bin`` directory may be used to download the required JARs into
    the ``lib`` directory. You should edit this script to match the versions used by your installation.

.. note::

    See :ref:`slf4j_configuration` for information about configuring the SLF4J implementation.

Due to licensing restrictions, dependencies for shape file support must be separately installed. Do this with
the following commands:

.. code-block:: bash

    $ bin/install-jai.sh
    $ bin/install-jline.sh

Test the command that invokes the GeoMesa Tools:

.. code::

    $ geomesa-lambda
    Usage: geomesa-lambda [command] [command options]
      Commands:
      ...

.. note::

    GeoMesa Accumulo command-line tools can be used against features which have been persisted to Accumulo.
    See :ref:`setting_up_accumulo_commandline` for details on the Accumulo command-line tools.

.. _install_lambda_geoserver:

Installing GeoMesa Lambda in GeoServer
--------------------------------------

.. warning::

    GeoMesa 2.2.x and 2.3.x require GeoServer 2.14.x. GeoMesa 2.1.x and earlier require GeoServer 2.12.x.

As described in section :ref:`geomesa_and_geoserver`, GeoMesa implements a `GeoTools`_-compatible data store.
This makes it possible to use GeoMesa as a data store in `GeoServer`_. GeoServer's web site includes
`installation instructions for GeoServer`_.

.. _installation instructions for GeoServer: http://docs.geoserver.org/stable/en/user/installation/index.html

After GeoServer is installed, you may install the WPS plugin if you plan to use GeoMesa processes. The GeoServer
WPS Plugin must match the version of the GeoServer instance. The GeoServer website includes instructions for
downloading and installing `the WPS plugin`_.

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


To install the GeoMesa Lambda data store as a GeoServer plugin, unpack the contents of the
``geomesa-lambda-gs-plugin_2.11-$VERSION-install.tar.gz`` file in ``geomesa-lambda_2.11-$VERSION/dist/geoserver/``
in the binary distribution or ``geomesa-$VERSION/geomesa-lambda/geomesa-lambda-gs-plugin/target/`` in the source
distribution into your GeoServer's ``lib`` directory (``$VERSION`` = |release|):

If you are using Tomcat:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-lambda_2.11-$VERSION/dist/geoserver/geomesa-lambda-gs-plugin_2.11-$VERSION-install.tar.gz \
      -C /path/to/tomcat/webapps/geoserver/WEB-INF/lib/

If you are using GeoServer's built in Jetty web server:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-lambda_2.11-$VERSION/dist/geoserver/geomesa-lambda-gs-plugin_2.11-$VERSION-install.tar.gz \
      -C /path/to/geoserver/webapps/geoserver/WEB-INF/lib/

There are additional JARs for Accumulo, Zookeeper, Hadoop, Thrift and Kafka that you will need to copy to GeoServer's
``WEB-INF/lib`` directory. The versions required will be specific to your installation. For example, GeoMesa only
requires Hadoop |hadoop_version|, but if you are using Hadoop 2.5.0 you should use the JARs
that match the version of Hadoop you are running.

.. warning::

   Due to a classpath conflict with GeoServer, the version of Accumulo client JARs installed must be 1.9.2 or later.
   Note that newer Accumulo clients can talk to older Accumulo instances, so it is only necessary to upgrade the
   client JARs in GeoServer, but not the entire Accumulo cluster.

There are scripts in the ``geomesa-lambda_2.11-$VERSION/bin`` directory
(``install-hadoop-accumulo.sh``, ``install-kafka.sh``) which will install these dependencies to a target directory
using ``curl`` (requires an internet connection).

.. note::

    You may have to edit ``install-hadoop-accumulo.sh`` and/or ``install-kafka.sh`` to set the
    versions of Accumulo, Zookeeper, Hadoop, Thrift and Kafka that you are running.

If you do no have an internet connection you can download the JARs manually via http://search.maven.org/.
These may include the JARs below; the specific JARs needed for some common configurations are listed below:

Accumulo 1.6

* accumulo-core-1.6.5.jar
* accumulo-fate-1.6.5.jar
* accumulo-server-base-1.6.5.jar
* accumulo-trace-1.6.5.jar
* accumulo-start-1.6.5.jar
* libthrift-0.9.1.jar
* zookeeper-3.4.6.jar
* commons-vfs2-2.0.jar

Accumulo 1.7+ (note the addition of htrace)

* accumulo-core-1.7.1.jar
* accumulo-fate-1.7.1.jar
* accumulo-server-base-1.7.1.jar
* accumulo-trace-1.7.1.jar
* accumulo-start-1.7.1.jar
* libthrift-0.9.1.jar
* zookeeper-3.4.6.jar
* htrace-core-3.1.0-incubating.jar
* commons-vfs2-2.1.jar

Hadoop 2.2

* commons-configuration-1.6.jar
* hadoop-auth-2.2.0.jar
* hadoop-client-2.2.0.jar
* hadoop-common-2.2.0.jar
* hadoop-hdfs-2.2.0.jar

Hadoop 2.4-2.7 (adjust versions as needed)

* commons-configuration-1.6.jar
* hadoop-auth-2.6.4.jar
* hadoop-client-2.6.4.jar
* hadoop-common-2.6.4.jar
* hadoop-hdfs-2.6.4.jar

Kafka 0.9.0.1

* kafka_2.11-0.9.0.1.jar
* kafka-clients-0.9.0.1.jar"
* zookeeper-3.4.5.jar"
* zkclient-0.7.jar"
* metrics-core-2.2.0.jar

Restart GeoServer after the JARs are installed.

Accumulo Versions
^^^^^^^^^^^^^^^^^

.. note::

    GeoMesa targets Accumulo 1.8 as a runtime dependency. Most artifacts will work with older versions
    of Accumulo without changes, however some artifacts which bundle Accumulo will need to be built manually.
    Accumulo 1.8 introduced a dependency on libthrift version 0.9.3 which is not compatible with Accumulo
    1.7/libthrift 0.9.1. To target an earlier Accumulo version, modify ``<accumulo.version>`` and
    ``<thrift.version>`` in the main pom.xml and re-build.

.. _install_geomesa_process_lambda:

GeoMesa Process
^^^^^^^^^^^^^^^

.. note::

    Some GeoMesa-specific WPS processes such as ``geomesa:Density``, which is used
    in the generation of heat maps, also require ``geomesa-process-wps_2.11-$VERSION.jar``.
    This JAR is included in the ``geomesa-lambda_2.11-$VERSION/dist/gs-plugins`` directory of the binary
    distribution, or is built in the ``geomesa-process`` module of the source
    distribution.

Upgrading
---------

To upgrade between minor releases of GeoMesa, the versions of all GeoMesa components
**must** match. This means that the version of the ``geomesa-distributed-runtime``
JAR installed on Accumulo tablet servers **must** match the version of the
``geomesa-plugin`` JARs installed in the ``WEB-INF/lib`` directory of GeoServer.

We strive to maintain backwards compatibility for data ingested with older
releases of GeoMesa, and in general data ingested with older releases
may be read with newer ones (note that the reverse does not apply). For example,
data ingested with GeoMesa 1.2.2 may be read with 1.2.3.

It should be noted, however, that data ingested with older GeoMesa versions may
not take full advantage of indexing improvements in newer releases. If
it is not feasible to reingest old data, see :ref:`update_index_format_job`
for more information on updating its index format.

