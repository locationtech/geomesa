Installing GeoMesa Accumulo
===========================

.. note::

    GeoMesa currently supports Accumulo version |accumulo_version|.

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
runtime JARs that contains server-side code for Accumulo that must be made
available on each of the Accumulo tablet servers in the cluster. These JARs
contain GeoMesa code and the Accumulo iterators required for querying GeoMesa data.

.. warning::

    GeoMesa requires commons-vfs2.jar 2.1 or later. This JAR ships with Accumulo 1.7.2+, but for older
    installations the JAR needs to be updated in ``$ACCUMULO_HOME/lib`` on all Accumulo servers.

.. warning::

    There are two runtime JARs available, with and without raster support. Only one is
    needed and including both will cause classpath issues.

The version of the distributed runtime JAR must match the version of the GeoMesa
data store client JAR (usually installed in GeoServer; see below). If not,
queries might not work correctly or at all.

.. _install_accumulo_runtime_manual:

Manual Install
^^^^^^^^^^^^^^

The desired runtime JAR should be copied into the ``$ACCUMULO_HOME/lib/ext`` folder on
each tablet server.

.. code-block:: bash

    # something like this for each tablet server
    $ scp dist/accumulo/geomesa-accumulo-distributed-runtime_2.11-$VERSION.jar \
        tserver1:$ACCUMULO_HOME/lib/ext
    # or for raster support
    $ scp dist/accumulo/geomesa-accumulo-raster-distributed-runtime_2.11-$VERSION.jar \
        tserver1:$ACCUMULO_HOME/lib/ext

.. note::

    You do not need the JAR on the Accumulo master server, and including
    it there may cause classpath issues later.

.. _install_accumulo_runtime_namespace:

Namespace Install
^^^^^^^^^^^^^^^^^

GeoMesa leverages namespaces and classpath contexts to isolate the GeoMesa
classpath from the rest of Accumulo.

To install the distributed runtime JAR, use the ``setup-namespace.sh``
script in the ``geomesa-accumulo_2.11-$VERSION/bin`` directory.

.. code::

    $ ./setup-namespace.sh -u myUser -n myNamespace

The command line arguments the script accepts are:

* -u <Accumulo username>
* -n <Accumulo namespace>
* -p <Accumulo password>
* -t <Use a cached Kerberos TGT>
* -g <Path of GeoMesa distributed runtime JAR> (optional, will default to the distribution folder and without raster support)
* -h <HDFS URI e.g. hdfs://localhost:9000> (optional, will attempt to determine if not supplied)

Since ``accumulo shell`` does not directly support Kerberos keytabs, if using Kerberos (``-t``) then a cached Kerberos
ticket-granting-ticket (TGT) should be obtained using the ``kinit`` command.

If ``-t`` is specified, ``-p`` must not be specified. If both ``-p`` and ``-t`` are omitted, the user is prompted for a password.

Alternatively you can manually install the distributed runtime JAR with these commands:

.. code::

    $ accumulo shell -u root
    > createnamespace myNamespace
    > grant NameSpace.CREATE_TABLE -ns myNamespace -u myUser
    > config -s general.vfs.context.classpath.myNamespace=hdfs://NAME_NODE_FDQN:54310/accumulo/classpath/myNamespace/[^.].*.jar
    > config -ns myNamespace -s table.classpath.context=myNamespace

Then copy the distributed runtime JAR into HDFS under the path you specified.
The path above is just an example; you can included nested folders with project
names, version numbers, and other information in order to have different versions of GeoMesa on
the same Accumulo instance. You should remove any GeoMesa JARs under
``$ACCUMULO_HOME/lib/ext`` to prevent any classpath conflicts.

.. note::

    When connecting to a data store using Accumulo namespaces, you must prefix
    the ``tableName`` parameter with the namespace. For example, refer to the
    ``my_catalog`` table as ``myNamespace.my_catalog``.

.. _setting_up_accumulo_commandline:

Setting up the Accumulo Command Line Tools
------------------------------------------

.. warning::

    To use the Accumulo data store with the command line tools, you need to install
    the distributed runtime first. See :ref:`install_accumulo_runtime`.

GeoMesa comes with a set of command line tools for managing Accumulo features located in ``geomesa-accumulo_2.11-$VERSION/bin/`` of the binary distribution.

.. note::

    You can configure environment variables and classpath settings in geomesa-accumulo_2.11-$VERSION/conf/geomesa-env.sh.

In the ``geomesa-accumulo_2.11-$VERSION`` directory, run ``bin/geomesa-accumulo configure`` to set up the tools.

.. code-block:: bash

    ### in geomesa-accumulo_2.11-$VERSION/:
    $ bin/geomesa-accumulo configure
    Warning: GEOMESA_ACCUMULO_HOME is not set, using /path/to/geomesa-accumulo_2.11-$VERSION
    Using GEOMESA_ACCUMULO_HOME as set: /path/to/geomesa-accumulo_2.11-$VERSION
    Is this intentional? Y\n y
    Warning: GEOMESA_LIB already set, probably by a prior configuration.
    Current value is /path/to/geomesa-accumulo_2.11-$VERSION/lib.

    Is this intentional? Y\n y

    To persist the configuration please update your bashrc file to include:
    export GEOMESA_ACCUMULO_HOME=/path/to/geomesa-accumulo_2.11-$VERSION
    export PATH=${GEOMESA_ACCUMULO_HOME}/bin:$PATH

Update and re-source your ``~/.bashrc`` file to include the ``$GEOMESA_ACCUMULO_HOME`` and ``$PATH`` updates.

.. warning::

    Please note that the ``$GEOMESA_ACCUMULO_HOME`` variable points to the location of the ``geomesa-accumulo_2.11-$VERSION``
    directory, not the main geomesa binary distribution directory.

.. note::

    ``geomesa-accumulo`` will read the ``$ACCUMULO_HOME`` and ``$HADOOP_HOME`` environment variables to load the
    appropriate JAR files for Hadoop, Accumulo, Zookeeper, and Thrift. If possible, we recommend
    installing the tools on the Accumulo master server, as you may also need various configuration
    files from Hadoop/Accumulo in order to run certain commands.

    GeoMesa provides the ability to provide additional jars on the classpath using the environmental variable
    ``$GEOMESA_EXTRA_CLASSPATHS``. GeoMesa will prepend the contents of this environmental variable  to the computed
    classpath giving it highest precedence in the classpath. Users can provide directories of jar files or individual
    files using a colon (``:``) as a delimiter. These entries will also be added the the map-reduce libjars variable.
    Use the ``geomesa-accumulo classpath`` command to print the final classpath that will be used when executing geomesa
    commands.

    If you are running the tools on a system without
    Accumulo installed and configured, the ``install-hadoop-accumulo.sh`` script
    in the ``bin`` directory may be used to download the needed Hadoop/Accumulo JARs into
    the ``lib`` directory. You should edit this script to match the versions used by your
    installation.

.. note::

    See :ref:`slf4j_configuration` for information about configuring the SLF4J implementation.

Due to licensing restrictions, dependencies for shape file support and raster
ingest must be separately installed. Do this with the following commands:

.. code-block:: bash

    $ bin/install-jai.sh
    $ bin/install-jline.sh

Test the command that invokes the GeoMesa Tools:

.. code::

    $ geomesa-accumulo
    Using GEOMESA_ACCUMULO_HOME = /path/to/geomesa-accumulo_2.11-$VERSION
    Usage: geomesa-accumulo [command] [command options]
      Commands:
      ...

For more details, see :ref:`accumulo_tools`.

.. _install_accumulo_geoserver:

Installing GeoMesa Accumulo in GeoServer
----------------------------------------

.. warning::

    GeoMesa 2.2.x and 2.3.x require GeoServer 2.14.x. GeoMesa 2.1.x and earlier require GeoServer 2.12.x.

As described in section :ref:`geomesa_and_geoserver`, GeoMesa implements a
`GeoTools`_-compatible data store. This makes it possible
to use GeoMesa Accumulo as a data store in `GeoServer`_.
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


To install GeoMesa's Accumulo data store as a GeoServer plugin, we can utilize the script ``manage-geoserver-plugins.sh`` in ``bin`` directory
of the GeoMesa Accumulo or GeoMesa Hadoop distributions. (``$VERSION`` = |release|)

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
    0 | geomesa-accumulo-gs-plugin_2.11-$VERSION
    1 | geomesa-blobstore-gs-plugin_2.11-$VERSION
    2 | geomesa-process-wps_2.11-$VERSION
    3 | geomesa-stream-gs-plugin_2.11-$VERSION

    Module(s) to install: 0 1
    0 | Installing geomesa-accumulo-gs-plugin_2.11-$VERSION-install.tar.gz
    1 | Installing geomesa-blobstore-gs-plugin_2.11-$VERSION-install.tar.gz
    Done

If you prefer to install the GeoMesa Accumulo GeoServer plugin manually, unpack the contents of the
``geomesa-accumulo-gs-plugin_2.11-$VERSION-install.tar.gz`` file in ``geomesa-accumulo_2.11-$VERSION/dist/geoserver/``
in the binary distribution or ``geomesa-$VERSION/geomesa-accumulo/geomesa-accumulo-gs-plugin/target/`` in the source distribution
into your GeoServer's ``lib`` directory (``$VERSION`` = |release|):

If you are using Tomcat:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-accumulo_2.11-$VERSION/dist/gs-plugins/geomesa-accumulo-gs-plugin_2.11-$VERSION-install.tar.gz \
      -C /path/to/tomcat/webapps/geoserver/WEB-INF/lib/

If you are using GeoServer's built in Jetty web server:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-accumulo_2.11-$VERSION/dist/gs-plugins/geomesa-accumulo-gs-plugin_2.11-$VERSION-install.tar.gz \
      -C /path/to/geoserver/webapps/geoserver/WEB-INF/lib/

There are additional JARs for Accumulo, Zookeeper, Hadoop, and Thrift that will
be specific to your installation that you will also need to copy to GeoServer's
``WEB-INF/lib`` directory. For example, GeoMesa only requires Hadoop
|hadoop_version|, but if you are using Hadoop 2.5.0 you should use the JARs
that match the version of Hadoop you are running.

.. warning::

   Due to a classpath conflict with GeoServer, the version of Accumulo client JARs installed must be 1.9.2 or later.
   Note that newer Accumulo clients can talk to older Accumulo instances, so it is only necessary to upgrade the
   client JARs in GeoServer, but not the entire Accumulo cluster.

There is a script in the ``geomesa-accumulo_2.11-$VERSION/bin`` directory
(``$GEOMESA_ACCUMULO_HOME/bin/install-hadoop-accumulo.sh``) which will install these
dependencies to a target directory using ``curl`` (requires an internet
connection).

.. note::

    You may have to edit the ``install-hadoop-accumulo.sh`` script to set the
    versions of Accumulo, Zookeeper, Hadoop, and Thrift you are running.

.. code-block:: bash

    $ $GEOMESA_ACCUMULO_HOME/bin/install-hadoop-accumulo.sh /path/to/tomcat/webapps/geoserver/WEB-INF/lib/

By default, JARs will be downloaded from Maven central. You may override this by setting the environment variable
``GEOMESA_MAVEN_URL``. If you do no have an internet connection you can download the JARs manually
via http://search.maven.org/.

The specific JARs needed for some common configurations are listed below:

.. tabs::

    .. tab:: Accumulo 1.9

        * accumulo-core-1.9.2.jar
        * accumulo-fate-1.9.2.jar
        * accumulo-server-base-1.9.2.jar
        * accumulo-trace-1.9.2.jar
        * accumulo-start-1.9.2.jar
        * libthrift-0.9.3.jar
        * zookeeper-3.4.10.jar
        * htrace-core-3.1.0-incubating.jar
        * commons-vfs2-2.1.jar

    .. tab:: Accumulo 1.7

        * accumulo-core-1.7.4.jar
        * accumulo-fate-1.7.4.jar
        * accumulo-server-base-1.7.4.jar
        * accumulo-trace-1.7.4.jar
        * accumulo-start-1.7.4.jar
        * libthrift-0.9.1.jar
        * zookeeper-3.4.10.jar
        * htrace-core-3.1.0-incubating.jar
        * commons-vfs2-2.1.jar

    .. tab:: Accumulo 1.6

        * accumulo-core-1.6.6.jar
        * accumulo-fate-1.6.6.jar
        * accumulo-server-base-1.6.6.jar
        * accumulo-trace-1.6.6.jar
        * accumulo-start-1.6.6.jar
        * libthrift-0.9.1.jar
        * zookeeper-3.4.10.jar
        * commons-vfs2-2.1.jar

.. tabs::

    .. tab:: Hadoop 2.6-2.9

        (adjust versions as needed)

        * commons-configuration-1.6.jar
        * hadoop-auth-2.8.4.jar
        * hadoop-client-2.8.4.jar
        * hadoop-common-2.8.4.jar
        * hadoop-hdfs-2.8.4.jar


Restart GeoServer after the JARs are installed.

Accumulo Versions
^^^^^^^^^^^^^^^^^

.. note::

    GeoMesa targets Accumulo 1.9 as a runtime dependency. Most artifacts will work with older versions
    of Accumulo without changes, however some artifacts which bundle Accumulo will need to be built manually.
    Accumulo 1.8 introduced a dependency on libthrift version 0.9.3 which is not compatible with Accumulo
    1.7/libthrift 0.9.1. To target an earlier Accumulo version, modify ``<accumulo.version>`` and
    ``<thrift.version>`` in the main pom.xml and re-build.

.. warning::

    There are severe issues with Accumulo versions 1.8.0-1.9.1 and the Accumulo team recommends immediately 
    upgrading to 1.9.2 or higher.

.. _install_geomesa_process:

GeoMesa Process
^^^^^^^^^^^^^^^

.. note::

    Some GeoMesa-specific WPS processes such as ``geomesa:Density``, which is used
    in the generation of heat maps, also require ``geomesa-process-wps_2.11-$VERSION.jar``.
    This JAR is included in the ``geomesa-accumulo_2.11-$VERSION/dist/gs-plugins`` directory of the binary
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
data ingested into Accumulo with GeoMesa 1.2.2 may be read with 1.2.3.

It should be noted, however, that data ingested with older GeoMesa versions may
not take full advantage of indexing improvements in newer releases. If
it is not feasible to reingest old data, see :ref:`update_index_format_job`
for more information on updating its index format.

