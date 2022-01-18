Installing GeoMesa Lambda
=========================

Installing from the Binary Distribution
---------------------------------------

GeoMesa Lambda artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

.. note::

  In the following examples, replace ``${TAG}`` with the corresponding GeoMesa version (e.g. |release_version|), and
  ``${VERSION}`` with the appropriate Scala plus GeoMesa versions (e.g. |scala_release_version|).

Extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa-${TAG}/geomesa-lambda_${VERSION}-bin.tar.gz"
    $ tar xvf geomesa-lambda_${VERSION}-bin.tar.gz
    $ cd geomesa-lambda_${VERSION}
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt  logs/

.. _lambda_install_source:

Building from Source
--------------------

GeoMesa Lambda may also be built from source. For more information refer to :ref:`building_from_source`
in the developer manual, or to the ``README.md`` file in the the source distribution.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa Lambda
distribution. If you have built from source, the distribution is created in the ``target`` directory of
``geomesa-lambda/geomesa-lambda``.

More information about developing with GeoMesa may be found in the :doc:`/developer/index`.

.. _install_lambda_runtime:

Installing the Accumulo Distributed Runtime Library
---------------------------------------------------

The Lambda data store requires the Accumulo data store distributed runtime to be installed. See
:ref:`install_accumulo_runtime`.

.. _setting_up_lambda_commandline:

Setting up the Lambda Command Line Tools
----------------------------------------

GeoMesa comes with a set of command line tools located in ``geomesa-lambda_${VERSION}/bin/`` of the binary distribution.

Configuring the Classpath
^^^^^^^^^^^^^^^^^^^^^^^^^

GeoMesa needs Accumulo, Hadoop and Kafka JARs on the classpath. These are not bundled by default, as they should match
the versions installed on the target system.

If the environment variables ``ACCUMULO_HOME``, ``HADOOP_HOME`` and ``KAFKA_HOME`` are set, then GeoMesa will load
the appropriate JARs and configuration files from those locations and no further configuration is required. Otherwise,
you will be prompted to download the appropriate JARs the first time you invoke the tools. Environment variables can be
specified in ``conf/*-env.sh`` and dependency versions can be specified in ``conf/dependencies.sh``.

In order to run map/reduce jobs, the Hadoop ``*-site.xml`` configuration files from your Hadoop installation
must be on the classpath. If ``HADOOP_HOME`` is not set, then copy them into ``geomesa-lamdba_${VERSION}/conf``.

GeoMesa also provides the ability to add additional JARs to the classpath using the environmental variable
``$GEOMESA_EXTRA_CLASSPATHS``. GeoMesa will prepend the contents of this environmental variable  to the computed
classpath, giving it highest precedence in the classpath. Users can provide directories of jar files or individual
files using a colon (``:``) as a delimiter. These entries will also be added the the map-reduce libjars variable.

.. note::

    See :ref:`slf4j_configuration` for information about configuring the SLF4J implementation.

Due to licensing restrictions, dependencies for shape file support must be separately installed.
Do this with the following command:

.. code-block:: bash

    $ ./bin/install-shapefile-support.sh

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

    See :ref:`geoserver_versions` to ensure that GeoServer is compatible with your GeoMesa version.

Installing GeoServer
^^^^^^^^^^^^^^^^^^^^

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

    The value of ``-Xmx`` should be as large as your system will permit. Be sure to restart
    Tomcat for changes to take place.


Installing the GeoMesa Lambda Data Store
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To install the GeoMesa data store, extract the contents of the
``geomesa-lambda-gs-plugin_${VERSION}-install.tar.gz`` file in ``geomesa-lambda_${VERSION}/dist/geoserver/``
in the binary distribution or ``geomesa-lambda/geomesa-lambda-gs-plugin/target/`` in the source
distribution into your GeoServer's ``lib`` directory:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-lambda_${VERSION}/dist/gs-plugins/geomesa-lambda-gs-plugin_${VERSION}-install.tar.gz \
      -C /path/to/geoserver/webapps/geoserver/WEB-INF/lib

Next, install the JARs for Accumulo, Hadoop and Kafka. By default, JARs will be downloaded from Maven central. You may
override this by setting the environment variable ``GEOMESA_MAVEN_URL``. If you do no have an internet connection
you can download the JARs manually via http://search.maven.org/.

Edit the file ``geomesa-lambda_${VERSION}/conf/dependencies.sh`` to set the versions of Accumulo, Hadoop and Kafka
to match the target environment, and then run the script:

.. code-block:: bash

    $ ./bin/install-dependencies.sh /path/to/geoserver/webapps/geoserver/WEB-INF/lib

.. warning::

   Due to a classpath conflict with GeoServer, the version of Accumulo client JARs installed must be 1.9.2 or later.
   Note that newer Accumulo clients can talk to older Accumulo instances, so it is only necessary to upgrade the
   client JARs in GeoServer, but not the entire Accumulo cluster.

.. warning::

    GeoServer ships with an older version of commons-text, 1.4. The ``install-dependencies.sh`` script will
    remove it, but if you don't use the script you will need to delete it manually.

The specific JARs needed for some common configurations are listed below:

Accumulo 1.7+

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
* hadoop-common-2.2.0.jar
* hadoop-hdfs-2.2.0.jar

Hadoop 2.4-2.7 (adjust versions as needed)

* commons-configuration-1.6.jar
* hadoop-auth-2.6.4.jar
* hadoop-common-2.6.4.jar
* hadoop-hdfs-2.6.4.jar

Kafka 0.9.0.1

* kafka_2.11-0.9.0.1.jar
* kafka-clients-0.9.0.1.jar"
* zookeeper-3.4.5.jar"
* zkclient-0.7.jar"
* metrics-core-2.2.0.jar

Restart GeoServer after the JARs are installed.

.. _install_geomesa_process_lambda:

GeoMesa Process
^^^^^^^^^^^^^^^

GeoMesa provides some WPS processes, such as ``geomesa:Density`` which is used to generate heat maps. In order
to use these processes, install the GeoServer WPS plugin as described in :ref:`geomesa_process`.

Upgrading
---------

To upgrade between minor releases of GeoMesa, the versions of all GeoMesa components
**must** match. This means that the version of the ``geomesa-distributed-runtime``
JAR installed on Accumulo tablet servers **must** match the version of the
``geomesa-plugin`` JARs installed in the ``WEB-INF/lib`` directory of GeoServer.

See :ref:`upgrade_guide` for more details on upgrading between versions.
