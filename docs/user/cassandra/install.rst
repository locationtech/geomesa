Installing GeoMesa Cassandra
============================

.. note::

    GeoMesa currently supports Cassandra version |cassandra_version|.

Connecting to Cassandra
-----------------------

The first step to getting started with Cassandra and GeoMesa is to install
Cassandra itself. You can find good directions for downloading and installing
Cassandra online. For example, see Cassandra's official `getting started`_ documentation.

.. _getting started: https://cassandra.apache.org/doc/latest/getting_started/index.html

Once you have Cassandra installed, the next step is to prepare your Cassandra installation
to integrate with GeoMesa. First, create a key space within Cassandra. The easiest way to
do this with ``cqlsh``, which should have been installed as part of your Cassandra installation.
Start ``cqlsh``, then type::

    CREATE KEYSPACE mykeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};

This creates a key space called "mykeyspace". This is a top-level name space within Cassandra
and it will provide a place for GeoMesa to put all of its data, including data for spatial features
and associated metadata.

Next, you'll need to set the ``CASSANDRA_HOME`` environment variable. GeoMesa uses this variable
to find the Cassandra jars. These jars should be in the ``lib`` directory of your Cassandra
installation. To set the variable add the following line to your ``.profile`` or ``.bashrc`` file::

    export CASSANDRA_HOME=/path/to/cassandra

Finally, make sure you know a contact point for your Cassandra instance.
If you are just trying things locally, and using the default Cassandra settings,
the contact point would be ``127.0.0.1:9042``. You can check and configure the
port you are using using the ``native_transport_port`` in the Cassandra
configuration file (located at ``conf/cassandra.yaml`` in your Cassandra
installation directory).

Installing from the Binary Distribution
---------------------------------------

GeoMesa Cassandra artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version
(|release|) from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

Extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa_2.11-$VERSION/geomesa-cassandra_2.11-$VERSION-bin.tar.gz"
    $ tar xvf geomesa-cassandra_2.11-$VERSION-bin.tar.gz
    $ cd geomesa-cassandra_2.11-$VERSION
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt  logs/

.. _cassandra_install_source:

Building from Source
--------------------

GeoMesa Cassandra may also be built from source. For more information refer to :ref:`building_from_source`
in the developer manual, or to the ``README.md`` file in the the source distribution.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa Cassandra
distribution. If you have built from source, the distribution is created in the ``target`` directory of
``geomesa-cassandra/geomesa-cassandra-dist``.

More information about developing with GeoMesa may be found in the :doc:`/developer/index`.

.. _setting_up_cassandra_commandline:

Setting up the Cassandra Command Line Tools
-------------------------------------------

GeoMesa Cassandra comes with a set of command line tools for managing Cassandra features located in
``geomesa-cassandra_2.11-$VERSION/bin/`` of the binary distribution.

.. note::

    You can configure environment variables and classpath settings in ``geomesa-cassandra_2.11-$VERSION/conf/geomesa-env.sh``.

.. note::

    ``geomesa-cassandra`` will read the ``$CASSANDRA_HOME`` and ``$HADOOP_HOME`` environment variables to load the
    appropriate JAR files for Cassandra and Hadoop. In addition, ``geomesa-cassandra`` will pull any
    additional jars from the ``$GEOMESA_EXTRA_CLASSPATHS`` environment variable into the class path.
    Use the ``geomesa classpath`` command in order to see what JARs are being used.

If you do not have a local Cassandra installation you will need to manually install the Cassandra JARs into the
tools ``lib`` folder. To do this, use the scripts provided with the distribution:

.. code-block:: bash

    $ bin/install-cassandra-jars.sh lib
    $ bin/install-tools-jar.sh lib

Due to licensing restrictions, dependencies for shape file support must be separately installed.
Do this with the following commands:

.. code-block:: bash

    $ bin/install-jai.sh
    $ bin/install-jline.sh

Run ``geomesa-cassandra`` without arguments to confirm that the tools work.

.. code::

    $ bin/geomesa-cassandra
    INFO  Usage: geomesa-cassandra [command] [command options]
      Commands:
      ...

.. _install_cassandra_geoserver:

Installing GeoMesa Cassandra in GeoServer
-----------------------------------------

.. warning::

    GeoMesa 2.2.x and 2.3.x require GeoServer 2.14.x. GeoMesa 2.1.x and earlier require GeoServer 2.12.x.

The GeoMesa Cassandra distribution includes a GeoServer plugin for including
Cassandra data stores in GeoServer. The plugin files are in the
``dist/gs-plugins/geomesa-cassandra-gs-plugin_2.11-$VERSION-install.tar.gz`` archive within the
GeoMesa Cassandra distribution directory.

To install the plugins, extract the archive and copy the contents to the ``WEB-INF/lib``
directory of your GeoServer installation. You will also need to install the Cassandra JARs; these
are not bundled to allow for different versions. The distribution includes a script to download
the JARs: ``bin/install-cassandra-jars.sh``. Call it with the path to the GeoServer ``WEB-INF/lib`` directory.
By default, it will install the following JARs:

 * cassandra-all-3.0.11.jar
 * cassandra-driver-core-3.0.0.jar
 * cassandra-driver-mapping-3.0.0.jar
 * netty-all-4.0.33.Final.jar
 * metrics-core-3.1.2.jar

Restart GeoServer after the JARs are installed.
