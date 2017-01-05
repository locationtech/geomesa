Installing GeoMesa Cassandra
============================

Installing Cassandra
--------------------

The first step to getting started with Cassandra and GeoMesa is to install
Cassandra itself. You can find good directions for downloading and installing
Cassandra online. For example, see Cassandra's official "Getting Started" documentation
here: https://cassandra.apache.org/doc/latest/getting_started/index.html .

Once you have Cassandra installed, the next step is to prepare your Cassandra installation
to integrate with GeoMesa. First, create a key space within Cassandra. The easiest way to
do this with ``cqlsh``, which should have been installed as part of your Cassandra installation.
Start ``cqlsh``, then type::

    CREATE KEYSPACE mykeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};

This creates a key space called "mykeyspace". This is a top-level name space within Cassandra
and it will provide a place for GeoMesa to put all of its data, including data for spatial features
and associated metadata.

Next, you'll need to set the ``CASSANDRA_LIB`` environment variable. GeoMesa uses this variable
to find the Cassandra jars. These jars should be in the ``lib`` directory of your Cassandra
installation. To set the variable add the following line to your ``.profile`` or ``.bashrc`` file::

    export CASSANDRA_LIB=/path/to/cassandra/lib

Finally, make sure you know a contact point for your Cassandra instance.
If you are just trying things locally, and using the default Cassandra settings,
the contact point would be ``127.0.0.1:9042``. You can check and configure the
port you are using using the ``native_transport_port`` in the Cassandra
configuration file (located at ``conf/cassandra.yaml`` in your Cassandra
installation directory).

Installing the GeoMesa Cassandra Data Store
-------------------------------------------

After you've successfully installed and configured Cassandra, you can
move on to installing the GeoMesa Cassandra distribution.
Start by choosing or creating a directory where you want
to put it. Then ``cd`` into that directory and type::

    wget https://repo.locationtech.org/content/repositories/geomesa-releases/org/locationtech/geomesa/geomesa-cassandra-dist_2.11/$VERSION/geomesa-cassandra-dist_2.11-$VERSION-bin.tar.gz
    tar xvf geomesa-cassandra-dist_2.11-$VERSION-bin.tar.gz

(Make sure to replace ``$VERSION`` with the version of the distribution that you want to use.
It should be something like ``1.3.0-m2``. You can browse available versions at
https://repo.locationtech.org/content/repositories/geomesa-releases/org/locationtech/geomesa/geomesa-cassandra-dist_2.11/ .)

The directory that you just extracted is the ``GEOMESA_CASSANDRA_HOME``. It includes all the files
that we'll need to use GeoMesa with Cassandra.

.. _install_cassandra_geoserver:

Installing GeoMesa Cassandra in GeoServer
-----------------------------------------

The GeoMesa Cassandra distribution includes a GeoServer plugin for including
Cassandra data stores in GeoServer. The plugin files are in the
``dist/gs-plugins/geomesa-cassandra-gs-plugin_2.11-$VERSION-install.tar.gz`` archive within the
GeoMesa Cassandra distribution directory.
To install the plugins, extract the archive and copy the contents to the ``WEB-INF/lib``
directory of your GeoServer installation. You will also need to copy the JARs from the
``lib`` directory of your Cassandra installation into the GeoServer ``WEB-INF/lib`` directory.

Restart GeoServer after the JARs are installed.