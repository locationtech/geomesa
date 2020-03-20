Installing GeoMesa Accumulo
===========================

.. note::

    GeoMesa currently supports Accumulo |accumulo_supported_versions|.

Installing from the Binary Distribution
---------------------------------------

GeoMesa Accumulo artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version
(|release|) from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

Extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-$VERSION/geomesa-accumulo_2.11-$VERSION-bin.tar.gz"
    $ tar xvf geomesa-accumulo_2.11-$VERSION-bin.tar.gz
    $ cd geomesa-accumulo_2.11-$VERSION
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt  logs/

.. _accumulo_install_source:

Building from Source
--------------------

GeoMesa Accumulo may also be built from source. For more information refer to :ref:`building_from_source`
in the developer manual, or to the ``README.md`` file in the the source distribution.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa Accumulo
distribution. If you have built from source, the distribution is created in the ``target`` directory of
``geomesa-accumulo/geomesa-accumulo-dist``.

More information about developing with GeoMesa may be found in the :doc:`/developer/index`.

.. _install_accumulo_runtime:

Installing the Accumulo Distributed Runtime Library
---------------------------------------------------

The ``geomesa-accumulo_2.11-$VERSION/dist/accumulo/`` directory contains the distributed
runtime JAR that contains server-side code for Accumulo that must be made
available on each of the Accumulo tablet servers in the cluster. This JAR
contains GeoMesa code and the Accumulo iterators required for querying GeoMesa data.

.. warning::

    GeoMesa requires commons-vfs2.jar 2.1 or later. This JAR ships with Accumulo 1.7.2+, but for older
    installations the JAR needs to be updated in ``$ACCUMULO_HOME/lib`` on all Accumulo servers.

The version of the distributed runtime JAR must match the version of the GeoMesa
data store client JAR (usually installed in GeoServer; see below). If not,
queries might not work correctly or at all.

.. _install_accumulo_runtime_namespace:

Namespace Install
^^^^^^^^^^^^^^^^^

GeoMesa leverages namespaces and classpath contexts to isolate the GeoMesa
classpath from the rest of Accumulo.

To install the distributed runtime JAR, first copy it into HDFS:

.. code-block:: bash

    $ hadoop fs -mkdir /accumulo/classpath/myNamespace
    $ hadoop fs -put \
      geomesa-accumulo_2.11-$VERSION/dist/accumulo/geomesa-accumulo-distributed-runtime_2.11-$VERSION.jar \
      /accumulo/classpath/myNamespace/

Then configure the namespace classpath through the Accumulo shell:

.. code-block:: bash

    $ accumulo shell -u root
    > createnamespace myNamespace
    > grant NameSpace.CREATE_TABLE -ns myNamespace -u myUser
    > config -s general.vfs.context.classpath.myNamespace=hdfs://NAME_NODE_FDQN:9000/accumulo/classpath/myNamespace/[^.].*.jar
    > config -ns myNamespace -s table.classpath.context=myNamespace

.. note::

    The path above is just an example; you can included nested folders with project
    names, version numbers, and other information in order to have different versions of GeoMesa on
    the same Accumulo instance. You should remove any GeoMesa JARs under
    ``$ACCUMULO_HOME/lib/ext`` to prevent classpath conflicts.

.. note::

    When connecting to a data store using Accumulo namespaces, you must prefix
    the ``accumulo.catalog`` parameter with the namespace. For example, refer to the
    ``my_catalog`` table as ``myNamespace.my_catalog``.

.. _install_accumulo_runtime_manual:

System Install
^^^^^^^^^^^^^^

Alternatively, the GeoMesa runtime JAR can be copied into the ``$ACCUMULO_HOME/lib/ext`` folder on
each tablet server.

.. warning::

    This approach is not recommended, as the GeoMesa classes will be on the global
    Accumulo classpath, and may conflict with other custom JARs installed in Accumulo.

.. code-block:: bash

    $ for tserver in $(cat $ACCUMULO_HOME/conf/tservers); do \
        scp dist/accumulo/geomesa-accumulo-distributed-runtime_2.11-$VERSION.jar \
        $tserver:$ACCUMULO_HOME/lib/ext; done

.. _setting_up_accumulo_commandline:

Setting up the Accumulo Command Line Tools
------------------------------------------

.. warning::

    To use the Accumulo data store with the command line tools, you need to install
    the distributed runtime first. See :ref:`install_accumulo_runtime`.

GeoMesa comes with a set of command line tools for managing Accumulo features located in
``geomesa-accumulo_2.11-$VERSION/bin/`` of the binary distribution.

GeoMesa requires ``java`` to be available on the default path.

Configuring the Classpath
^^^^^^^^^^^^^^^^^^^^^^^^^

GeoMesa needs Accumulo and Hadoop JARs on the classpath. These are not bundled by default, as they should match
the versions installed on the target system.

If the environment variables ``$ACCUMULO_HOME`` and ``$HADOOP_HOME`` are set, then GeoMesa will load the appropriate
JARs and configuration files from those locations and no further configuration is required. For simplicity,
environment variables can be specified in ``geomesa-accumulo_2.11-$VERSION/conf/geomesa-env.sh``.

Alternatively, the necessary JARs can be installed by modifying the version numbers in
``geomesa-accumulo_2.11-$VERSION/bin/install-hadoop-accumulo.sh`` to match the target system, and then running it:

.. code-block:: bash

    $ cd geomesa-accumulo_2.11-$VERSION/bin
    $ ./install-hadoop-accumulo.sh

In order to run map/reduce jobs, you will also need to copy the Hadoop ``*-site.xml`` configuration files
from your Hadoop installation into ``geomesa-accumulo_2.11-$VERSION/conf``.

GeoMesa also provides the ability to add additional JARs to the classpath using the environmental variable
``$GEOMESA_EXTRA_CLASSPATHS``. GeoMesa will prepend the contents of this environmental variable  to the computed
classpath, giving it highest precedence in the classpath. Users can provide directories of jar files or individual
files using a colon (``:``) as a delimiter. These entries will also be added the the map-reduce libjars variable.

Due to licensing restrictions, dependencies for shape file support must be separately installed. Do this with
the following commands:

.. code-block:: bash

    $ bin/install-jai.sh
    $ bin/install-jline.sh

For logging, see :ref:`slf4j_configuration` for information about configuring the SLF4J implementation.

Use the ``geomesa-accumulo classpath`` command to print the final classpath that will be used when executing GeoMesa
commands.

Configuring the Path
^^^^^^^^^^^^^^^^^^^^

In order to be able to run the ``geomesa-accumulo`` command from anywhere, you can set the environment
variable ``GEOMESA_ACCUMULO_HOME`` and add it to your path by modifying your bashrc file:

.. code-block:: bash

    $ echo 'export GEOMESA_ACCUMULO_HOME=/path/to/geomesa-accumulo_2.11-$VERSION' >> ~/.bashrc
    $ echo 'export PATH=${GEOMESA_ACCUMULO_HOME}/bin:$PATH' >> ~/.bashrc
    $ source ~/.bashrc
    $ which geomesa-accumulo
    /path/to/geomesa-accumulo_2.11-$VERSION/bin/geomesa-accumulo

Running Commands
^^^^^^^^^^^^^^^^

Test the command that invokes the GeoMesa Tools:

.. code::

    $ geomesa-accumulo
    Usage: geomesa-accumulo [command] [command options]
      Commands:
      ...

For details on the available commands, see :ref:`accumulo_tools`.

.. _install_accumulo_geoserver:

Installing GeoMesa Accumulo in GeoServer
----------------------------------------

.. warning::

    See :ref:`geoserver_versions` to ensure that GeoServer is compatible with your GeoMesa version.

Installing GeoServer
^^^^^^^^^^^^^^^^^^^^

As described in :ref:`geomesa_and_geoserver`, GeoMesa implements a `GeoTools`_-compatible data store. This makes
it possible to use GeoMesa Accumulo as a data store in `GeoServer`_. GeoServer's web site includes
`installation instructions for GeoServer`_.

.. _installation instructions for GeoServer: http://docs.geoserver.org/stable/en/user/installation/index.html

After GeoServer is running, you may optionally install the WPS plugin. The GeoServer WPS Plugin must match the
version of your GeoServer instance. The GeoServer website includes instructions for downloading
and installing `the WPS plugin`_.

.. _the WPS plugin: http://docs.geoserver.org/stable/en/user/services/wps/install.html

.. note::

    If using Tomcat as a web server, it will most likely be necessary to
    pass some custom options::

        export CATALINA_OPTS="-Xmx8g -XX:MaxPermSize=512M -Duser.timezone=UTC \
        -server -Djava.awt.headless=true"

    The value of ``-Xmx`` should be as large as your system will permit. Be sure to
    restart Tomcat for changes to take place.

Installing the GeoMesa Accumulo Data Store
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To install the GeoMesa data store, extract the contents of the
``geomesa-accumulo-gs-plugin_2.11-$VERSION-install.tar.gz`` file in ``geomesa-accumulo_2.11-$VERSION/dist/geoserver/``
in the binary distribution or ``geomesa-accumulo/geomesa-accumulo-gs-plugin/target/`` in the source
distribution into your GeoServer's ``lib`` directory:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-accumulo_2.11-$VERSION/dist/gs-plugins/geomesa-accumulo-gs-plugin_2.11-$VERSION-install.tar.gz \
      -C /path/to/geoserver/webapps/geoserver/WEB-INF/lib

Next, install the JARs for Accumulo and Hadoop. By default, JARs will be downloaded from Maven central. You may
override this by setting the environment variable ``GEOMESA_MAVEN_URL``. If you do no have an internet connection
you can download the JARs manually via http://search.maven.org/.

Edit the script ``geomesa-accumulo_2.11-$VERSION/bin/install-hadoop-accumulo.sh`` to match the versions of the
target environment, and then run the script:

.. code-block:: bash

    $ ./install-hadoop-accumulo.sh /path/to/geoserver/webapps/geoserver/WEB-INF/lib

.. warning::

   Due to a classpath conflict with GeoServer, the version of Accumulo client JARs installed must be 1.9.2 or later.
   Note that newer Accumulo clients can talk to older Accumulo instances, so it is only necessary to upgrade the
   client JARs in GeoServer, but not the entire Accumulo cluster.

.. warning::

    GeoServer ships with an older version of commons-text, 1.4. The ``install-hadoop-accumulo.sh`` script will
    remove it, but if you don't use the script you will need to delete it manually.

The specific JARs needed for some common configurations are listed below:

.. tabs::

    .. tab:: Accumulo 2.0

        * accumulo-core-2.0.0.jar
        * accumulo-server-base-2.0.0.jar
        * accumulo-start-2.0.0.jar
        * commons-configuration-1.6.jar
        * commons-configuration2-2.5.jar
        * commons-logging-1.1.3.jar
        * commons-text-1.6.jar
        * commons-vfs2-2.3.jar
        * hadoop-auth-2.8.5.jar
        * hadoop-client-2.8.5.jar
        * hadoop-common-2.8.5.jar
        * hadoop-hdfs-2.8.5.jar
        * htrace-core-3.1.0-incubating.jar
        * htrace-core4-4.1.0-incubating.jar
        * libthrift-0.12.0.jar
        * zookeeper-3.4.14.jar

    .. tab:: Accumulo 1.9

        * accumulo-core-1.9.3.jar
        * accumulo-fate-1.9.3.jar
        * accumulo-server-base-1.9.3.jar
        * accumulo-start-1.9.3.jar
        * accumulo-trace-1.9.3.jar
        * commons-configuration-1.6.jar
        * commons-vfs2-2.1.jar
        * hadoop-auth-2.8.5.jar
        * hadoop-client-2.8.5.jar
        * hadoop-common-2.8.5.jar
        * hadoop-hdfs-2.8.5.jar
        * htrace-core-3.1.0-incubating.jar
        * libthrift-0.9.3.jar
        * zookeeper-3.4.14.jar

    .. tab:: Accumulo 1.7

        * accumulo-core-1.7.4.jar
        * accumulo-fate-1.7.4.jar
        * accumulo-server-base-1.7.4.jar
        * accumulo-start-1.7.4.jar
        * accumulo-trace-1.7.4.jar
        * commons-configuration-1.6.jar
        * commons-vfs2-2.1.jar
        * hadoop-auth-2.8.5.jar
        * hadoop-client-2.8.5.jar
        * hadoop-common-2.8.5.jar
        * hadoop-hdfs-2.8.5.jar
        * htrace-core-3.1.0-incubating.jar
        * libthrift-0.9.1.jar
        * zookeeper-3.4.14.jar

Restart GeoServer after the JARs are installed.

.. _install_geomesa_process:

Install GeoMesa Processes
^^^^^^^^^^^^^^^^^^^^^^^^^

GeoMesa provides some WPS processes, such as ``geomesa:Density`` which is used to generate heat maps. In order
to use these processes, copy the ``geomesa-process-wps_2.11-$VERSION.jar`` into GeoServer's ``WEB-INF/lib`` directory.
This JAR is included in the ``geomesa-accumulo_2.11-$VERSION/dist/gs-plugins`` directory of the binary
distribution, or is built in the ``geomesa-process/geomesa-process-wps`` module of the source distribution.

Upgrading
---------

To upgrade between minor releases of GeoMesa, the versions of all GeoMesa components
**must** match. This means that the version of the ``geomesa-distributed-runtime``
JAR installed on Accumulo tablet servers **must** match the version of the
``geomesa-plugin`` JARs installed in the ``WEB-INF/lib`` directory of GeoServer.

We strive to maintain backwards compatibility for data ingested with older
releases of GeoMesa, and in general data ingested with older releases
may be read with newer ones (note that the reverse does not apply). For example,
data ingested into Accumulo with GeoMesa 1.2.2 may be read with 1.2.3.

It should be noted, however, that data ingested with older GeoMesa versions may
not take full advantage of indexing improvements in newer releases. If
it is not feasible to reingest old data, see :ref:`update_index_format_job`
for more information on updating its index format.

