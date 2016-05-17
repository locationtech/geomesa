Installation and Configuration
==============================

This chapter describes how to install and configure GeoMesa on a Linux system.
This includes installation of the Accumulo and Kafka data stores and
installation and configuration of the GeoServer plugins.

Prerequisites and Platform
--------------------------

.. warning::

    GeoMesa requires `Accumulo <http://accumulo.apache.org/>`_ |accumulo_version|, which in turn
    requires `Hadoop <http://hadoop.apache.org/>`_ |hadoop_version| and `ZooKeeper <http://zookeeper.apache.org>`_
    |zookeeper_version|. Installing and configuring Accumulo is beyond the scope of this manual.

    Using the `Kafka <http://kafka.apache.org/>`_ module requires Kafka |kafka_version| and ZooKeeper |zookeeper_version|.  

To install the binary distribution:

* `Java JRE or JDK 8 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__,

To build and install the source distribution:

* `Java JDK 8 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__,
* Apache Maven (http://maven.apache.org/) |maven_version|
* A ``git`` client (http://git-scm.com/)

Versions and Downloads
----------------------

.. note::

    The current recommended version of GeoMesa to install is |release|.

**Latest release**: |release|

.. TODO: substitutions don't work in some kinds of markup, including URLs

* Release tarball: |release_tarball|
* Source: |release_source_tarball|

**Development version (source only)**: |development|

* Source: https://github.com/locationtech/geomesa/archive/master.tar.gz

**Latest release which has been fully reviewed by Eclipse Legal**: |eclipse_release|

GeoMesa is part of the Locationtech working group at Eclipse. The Eclipse legal team fully reviews each major release for IP concerns.

.. warning::

    Eclipse releases may not contain all the bug fixes and improvements from the latest release.

* Release distribution: |eclipse_release_tarball|
* Source: |eclipse_release_source_tarball|

**1.1.x release**: |release_1_1|

* Release tarball: |release_1_1_tarball|
* Source: |release_1_1_source_tarball|

GeoMesa artifacts can be downloaded from the LocationTech Maven repository: https://repo.locationtech.org/content/repositories/geomesa-releases/.

Snapshot artifacts are available in the LocationTech snapshots repository: https://repo.locationtech.org/content/repositories/geomesa-snapshots/.

.. _install_binary:

Installing from the Binary Distribution
---------------------------------------

GeoMesa artifacts are available for download or can be built from source. 
The easiest way to get started is to download the most recent binary version (``$VERSION`` = |release|) 
and untar it somewhere convenient.

.. code-block:: bash

    # download and unpackage the most recent distribution
    $ wget http://repo.locationtech.org/content/repositories/geomesa-releases/org/locationtech/geomesa/geomesa-dist/$VERSION/geomesa-dist-$VERSION-bin.tar.gz
    $ tar xvf geomesa-dist-$VERSION-bin.tar.gz
    $ cd geomesa-$VERSION
    $ ls
    dist/  docs/  LICENSE.txt  README.md

Building from Source
--------------------

GeoMesa may also be built from source. For more information refer to :ref:`building_from_source`
in the :doc:`/developer/index`, or to the ``README.md`` file in the the
source distribution. The remainder of the instructions in this chapter assume
the use of the binary GeoMesa distribution. If you have built from source, the
distribution is created in the ``geomesa-dist/target`` directory as a part of
the build process.

More information about developing with GeoMesa may be found in the :doc:`/developer/index`.

.. _setting_up_commandline:

Setting up the Command Line Tools
---------------------------------

GeoMesa comes with a set of command line tools for managing features. To complete the setup 
of the tools, `cd` into the ``dist/tools`` directory of the binary distribution and unpack the
``geomesa-tools-$VERSION-bin.tar.gz`` file (``$VERSION`` = |release|).

.. code-block:: bash

    $ cd geomesa-$VERSION/dist/tools
    $ tar -xzvf geomesa-tools-$VERSION-bin.tar.gz
    $ cd geomesa-tools-$VERSION
    $ ls
    bin/  conf/  examples/  lib/

The instructions below assume that the ``geomesa-tools-$VERSION`` directory is kept in the 
``geomesa-$VERSION/dist/tools`` directory, but the tools distribution may be moved elsewhere
as desired.

In the ``geomesa-tools-$VERSION`` directory, run ``bin/geomesa configure`` to set up the tools.

.. code-block:: bash

    ### in geomesa-$VERSION/dist/tools/geomesa-tools-$VERSION:
    $ bin/geomesa configure
    Warning: GEOMESA_HOME is not set, using /path/to/geomesa-$VERSION/dist/tools/geomesa-tools-$VERSION
    Using GEOMESA_HOME as set: /path/to/geomesa-$VERSION/dist/tools/geomesa-tools-$VERSION
    Is this intentional? Y\n y
    Warning: GEOMESA_LIB already set, probably by a prior configuration.
    Current value is /path/to/geomesa-$VERSION/dist/tools/geomesa-tools-$VERSION/lib.

    Is this intentional? Y\n y

    To persist the configuration please update your bashrc file to include: 
    export GEOMESA_HOME=/path/to/geomesa-$VERSION/dist/tools/geomesa-tools-$VERSION
    export PATH=${GEOMESA_HOME}/bin:$PATH

Update and re-source your ``~/.bashrc`` file to include the ``$GEOMESA_HOME`` and ``$PATH`` updates.

.. warning::

    Please note that the ``$GEOMESA_HOME`` variable points to the location of the ``geomesa-tools-$VERSION``
    directory, not the main geomesa binary distribution directory!

.. note::

    ``geomesa`` will read the ``$ACCUMULO_HOME`` and ``$HADOOP_HOME`` environment variables to load the
    appropriate JAR files for Hadoop, Accumulo, Zookeeper, and Thrift. If possible, we recommend
    installing the tools on the Accumulo master server, as you may also need various configuration
    files from Hadoop/Accumulo in order to run certain commands. Use the ``geomesa classpath``
    command in order to see what JARs are being used.

    If you are running the tools on a system without
    Accumulo installed and configured, the ``install-hadoop-accumulo.sh`` script
    in the ``bin`` directory may be used to download the needed Hadoop/Accumulo JARs into
    the ``lib`` directory. You should edit this script to match the versions used by your
    installation.

Due to licensing restrictions, dependencies for shape file support and raster
ingest must be separately installed. Do this with the following commands: 

.. code-block:: bash

    $ bin/install-jai
    $ bin/install-jline

Test the command that invokes the GeoMesa Tools:

.. code-block:: bash

    $ geomesa
    Using GEOMESA_HOME = /path/to/geomesa-$VERSION
    Usage: geomesa [command] [command options]
      Commands:
        create           Create a feature definition in a GeoMesa catalog
        deletecatalog    Delete a GeoMesa catalog completely (and all features in it)
        deleteraster     Delete a GeoMesa Raster Table
        describe         Describe the attributes of a given feature in GeoMesa
        env              Examine the current GeoMesa environment
        explain          Explain how a GeoMesa query will be executed
        export           Export a GeoMesa feature
        getsft           Get the SimpleFeatureType of a feature
        help             Show help
        ingest           Ingest a file of various formats into GeoMesa
        ingestraster     Ingest a raster file or raster files in a directory into GeoMesa
        list             List GeoMesa features for a given catalog
        queryrasterstats Export queries and statistics about the last X number of queries to a CSV file.
        removeschema     Remove a schema and associated features from a GeoMesa catalog
        tableconf        Perform table configuration operations
        version          GeoMesa Version


GeoMesa Tools comes bundled by default with an SLF4J implementation that is installed to the ``$GEOMESA_HOME/lib`` directory
named ``slf4j-log4j12-1.7.5.jar``. If you already have an SLF4J implementation installed on your Java classpath you may
see errors at runtime and will have to exclude one of the JARs. This can be done by simply renaming the bundled
``slf4j-log4j12-1.7.5.jar`` file to ``slf4j-log4j12-1.7.5.jar.exclude``.
 
Note that if no slf4j implementation is installed you will see this error:

.. code::

    SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
    SLF4J: Defaulting to no-operation (NOP) logger implementation
    SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.

In this case you may download SLF4J from http://www.slf4j.org/download.html. Extract 
``slf4j-log4j12-1.7.7.jar`` and place it in the ``lib`` directory of the binary distribution. 
If this conflicts with another SLF4J implementation, you may need to remove it from the ``lib`` directory.


Installing the Accumulo distributed runtime library
---------------------------------------------------

The ``geomesa-$VERSION/dist/accumulo`` directory contains the distributed
runtime JAR that contains server-side code for Accumulo that must be made
available on each of the Accumulo tablet servers in the cluster. This JAR
contains GeoMesa code and the Accumulo iterator required for querying 
GeoMesa data.

The version of the distributed runtime JAR must match the version of the GeoMesa
data store client JAR (usually installed in GeoServer; see below). If not,
queries might not work correctly or at all.

For Accumulo 1.5
^^^^^^^^^^^^^^^^

The runtime JAR should be copied into the ``$ACCUMULO_HOME/lib/ext`` folder on
each tablet server. 

.. code-block:: bash

    # something like this for each tablet server
    $ scp geomesa-$VERSION/dist/accumulo/geomesa-accumulo-distributed-runtime-$VERSION.jar tserver1:$ACCUMULO_HOME/lib/ext

.. note::

    You do not need the JAR on the Accumulo master server, and including
    it there may cause classpath issues later.

For Accumulo 1.6+
^^^^^^^^^^^^^^^^^

Copying the runtime JAR to each tablet server as for Accumulo 1.5 above will
still work, but in Accumulo 1.6, we can leverage namespaces to isolate the
GeoMesa classpath from the rest of Accumulo. First, you have to create the
namespace in the Accumulo shell:

.. code::

    $ accumulo shell -u root
    > createnamespace myNamespace
    > grant NameSpace.CREATE_TABLE -ns myNamespace -u myUser
    > config -s general.vfs.context.classpath.myNamespace=hdfs://NAME_NODE_FDQN:54310/accumulo/classpath/myNamespace/[^.].*.jar
    > config -ns myNamespace -s table.classpath.context=myNamespace

.. note::

    Depending on Hadoop version, you may need to use ``hdfs://NAME_NODE_FDQN:8020``.

Then copy the distributed runtime jar into HDFS under the path you specified.
The path above is just an example; you can included nested folders with project
names, version numbers, and other information in order to have different versions of GeoMesa on
the same Accumulo instance. You should remove any GeoMesa JARs under
``$ACCUMULO_HOME/lib/ext`` to prevent any classpath conflicts.

.. note::

    When connecting to a data store using Accumulo namespaces, you must prefix
    the ``tableName`` parameter with the namespace. For example, refer to the 
    ``my_catalog`` table as ``myNamespace.my_catalog``.

.. _install_geoserver_plugins:

Installing the GeoMesa GeoServer plugins
----------------------------------------

.. warning::

    The GeoMesa GeoServer plugins require the use of GeoServer
    |geoserver_version| and GeoTools |geotools_version|.


As described in section :ref:`geomesa_and_geoserver`, GeoMesa implements a 
`GeoTools <http://geotools.org/>`_-compatible data store. This makes it possible
to use GeoMesa as a data store in `GeoServer <http://geoserver.org>`_. The documentation
below describes how to configure GeoServer to connect to GeoMesa Accumulo and Kafka data stores.
GeoServer's web site includes `installation instructions for GeoServer <http://docs.geoserver.org/latest/en/user/installation/index.html>`_.

After GeoServer is running, you will also need to install the WPS plugin to
your GeoServer instance. The GeoServer WPS Plugin must match the version of
GeoServer instance. This is needed for both the Accumulo and Kafka variants of
the plugin. The GeoServer website includes `instructions for downloading and installing <http://docs.geoserver.org/stable/en/user/extensions/wps/install.html>`_ the WPS plugin.

.. note::

    If using Tomcat as a web server, it will most likely be necessary to
    pass some custom options::

        export CATALINA_OPTS="-Xmx8g -XX:MaxPermSize=512M -Duser.timezone=UTC -server -Djava.awt.headless=true"

    The value of ``-Xmx`` should be as large as your system will permit; this
    is especially important for the Kafka plugin. You
    should also consider passing ``-DGEOWEBCACHE_CACHE_DIR=/tmp/$USER-gwc``
    and ``-DEPSG-HSQL.directory=/tmp/$USER-hsql``
    as well. Be sure to restart Tomcat for changes to take place.

For Accumulo
^^^^^^^^^^^^

To install the GeoMesa Accumulo GeoServer plugin, unpack the contents of the
``geomesa-accumulo-gs-plugin-$VERSION.tar.gz`` file in ``geomesa-$VERSION/dist/gs-plugins`` 
into your GeoServer's ``lib`` directory (``$VERSION`` = |release|):

If you are using Tomcat:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-$VERSION/dist/gs-plugins/geomesa-accumulo-gs-plugin-$VERSION-install.tar.gz \
      -C /path/to/tomcat/webapps/geoserver/WEB-INF/lib/

If you are using GeoServer's built in Jetty web server:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-$VERSION/dist/gs-plugins/geomesa-accumulo-gs-plugin-$VERSION-install.tar.gz \
      -C /path/to/geoserver/webapps/geoserver/WEB-INF/lib/

There are additional JARs for Accumulo, Zookeeper, Hadoop, and Thrift that will
be specific to your installation that you will also need to copy to GeoServer's
``WEB-INF/lib`` directory. For example, GeoMesa only requires Hadoop
|hadoop_version|, but if you are using Hadoop 2.5.0 you should use the JARs
that match the version of Hadoop you are running.

There is a script in the ``geomesa-tools-$VERSION`` directory
(``$GEOMESA_HOME/bin/install-hadoop-accumulo.sh``) which will install these
dependencies to a target directory using ``wget`` (requires an internet
connection).

.. note::

    You may have to edit the ``install-hadoop-accumulo.sh`` script to set the
    versions of Accumulo, Zookeeper, Hadoop, and Thrift you are running.

.. code-block:: bash

    $ $GEOMESA_HOME/bin/install-hadoop-accumulo.sh /path/to/tomcat/webapps/geoserver/WEB-INF/lib/
    Install accumulo and hadoop dependencies to /path/to/tomcat/webapps/geoserver/WEB-INF/lib/?
    Confirm? [Y/n]y
    fetching https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-core/1.6.2/accumulo-core-1.6.2.jar
    --2015-09-29 15:06:48--  https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-core/1.6.2/accumulo-core-1.6.2.jar
    Resolving search.maven.org (search.maven.org)... 207.223.241.72
    Connecting to search.maven.org (search.maven.org)|207.223.241.72|:443... connected.
    HTTP request sent, awaiting response... 200 OK
    Length: 4646545 (4.4M) [application/java-archive]
    Saving to: ‘/path/to/tomcat/webapps/geoserver/WEB-INF/lib/accumulo-core-1.6.2.jar’
    ...

If you do no have an internet connection you can download the JARs manually via http://search.maven.org/.
These may include the JARs below; the specific JARs needed for some common configurations are listed below:

Accumulo 1.5

* accumulo-core-1.5.3.jar
* accumulo-fate-1.5.3.jar
* accumulo-start-1.5.3.jar
* accumulo-trace-1.5.3.jar
* libthrift-0.9.0.jar
* zookeeper-3.3.6.jar

Accumulo 1.6

* accumulo-core-1.6.4.jar
* accumulo-fate-1.6.4.jar
* accumulo-server-base-1.6.4.jar
* accumulo-trace.1.6.4.jar
* libthrift-0.9.1.jar
* zookeeper-3.4.5.jar

Hadoop 2.2

* commons-configuration-1.6.jar
* hadoop-auth-2.2.0.jar
* hadoop-common-2.2.0.jar
* hadoop-hdfs-2.2.0.jar

Hadoop 2.4

* commons-configuration-1.6.jar
* hadoop-auth-2.4.0.jar
* hadoop-common-2.4.0.jar
* hadoop-hdfs-2.4.0.jar

For Kafka
^^^^^^^^^

To install the GeoMesa Kafka GeoServer plugin, unpack the contents of the
``geomesa-kafka-gs-plugin-$VERSION.tar.gz`` file in ``geomesa-$VERSION/dist/gs-plugins`` 
into your GeoServer's ``lib`` directory (``$VERSION`` = |release|):

If you are using Tomcat:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-$VERSION/dist/gs-plugins/geomesa-kafka-gs-plugin-$VERSION-install.zip \
      -C /path/to/tomcat/webapps/geoserver/WEB-INF/lib/

If you are using GeoServer's built in Jetty web server:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-$VERSION/dist/gs-plugins/geomesa-kafka-gs-plugin-$VERSION-install.zip \
      -C /path/to/geoserver/webapps/geoserver/WEB-INF/lib/

This will install the JARs for the Kafka GeoServer plugin and most of its dependencies.
However, you will also need additional JARs for Kafka and Zookeeper that will
be specific to your installation that you will also need to copy to GeoServer's
``WEB-INF/lib`` directory. For example, GeoMesa only requires Kafka |kafka_version|,
but if you are using Kafka 0.9.0 you should use the JARs that match the version of
Kafka you are running.

.. warning::

    GeoMesa |release| depends on Scala 2.11, so you should make sure you use the
    Kafka version built with Scala 2.11 as well (``kafka_2.11_*``) to avoid
    compatibility issues.

Copy these additional dependencies (or the equivalents for your Kafka installation) to
your GeoServer ``WEB-INF/lib`` directory.

* Kafka
    * kafka-clients-0.8.2.1.jar
    * kafka_2.11-0.8.2.1.jar
    * metrics-core-2.2.0.jar
    * zkclient-0.3.jar
* Zookeeper
    * zookeeper-3.4.5.jar

There is a script in the ``geomesa-tools-$VERSION`` directory
(``$GEOMESA_HOME/bin/install-kafka.sh``) which will install these
dependencies to a target directory using ``wget`` (requires an internet
connection).

GeoMesa GeoServer Community Module
----------------------------------

The GeoMesa community module adds support for raster imagery to GeoServer. The community module
requires the Accumulo GeoServer plugin to be installed first.

The community module can be downloaded from `OpenGeo <http://ares.opengeo.org/geoserver/>`__, or can
be built from `source <https://github.com/geoserver/geoserver/tree/master/src/community/geomesa>`__.

Once obtained, the community module can be installed by copying ``geomesa-gs-<version>.jar`` into
the GeoServer ``lib`` directory.

Upgrading
---------

To upgrade between minor releases of GeoMesa, the versions of all GeoMesa components **must** match. 

This means that the version of the ``geomesa-distributed-runtime`` JAR installed on Accumulo tablet servers
**must** match the version of the ``geomesa-plugin`` JARs installed in the ``WEB-INF/lib`` directory of GeoServer.

Configuring GeoServer
---------------------

Depending on your hardware, it may be important to set the limits for
your WMS plugin to be higher or disable them completely by clicking
"WMS" under "Services" on the left side of the admin page of GeoServer.
Check with your server administrator to determine the correct settings.
For massive queries, the standard 60 second timeout may be too short.

|"Disable limits"|

.. |"Disable limits"| image:: _static/img/wms_limits.png

To enable explain query logging in GeoServer, add the following to the
``$GEOSERVER_DATA_DIR/logs/DEFAULT_LOGGING.properties`` file::

    log4j.category.org.locationtech.geomesa.accumulo.index.QueryPlanner=TRACE

If you are not sure of the location of your GeoServer data directory, it
is printed out when you start GeoServer::

    ----------------------------------
    - GEOSERVER_DATA_DIR: /opt/devel/install/geoserver-data-dir
    ----------------------------------

Apache Commons Collections
--------------------------

Version 3.2.1 and earlier of the Apache Commons Collections library have a CVSS 10.0 vulnerability.  Read more `here
https://commons.apache.org/proper/commons-collections/security-reports.html`__.

Note that Accumulo 1.6.5 is the first version of Accumulo which addresses this security concern.
Fixes for the GeoServer project will be available in versions 2.8.3+ and 2.9.0+.
