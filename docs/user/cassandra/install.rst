Installing GeoMesa Cassandra
============================

.. note::

    GeoMesa currently supports Cassandra version |cassandra_version|.

.. note::

    The examples below expect a version to be set in the environment:

    .. parsed-literal::

        $ export TAG="|release_version|"
<<<<<<< HEAD
        $ export VERSION="|scala_binary_version|-${TAG}" # note: |scala_binary_version| is the Scala build version
=======
        # note: |scala_binary_version| is the Scala build version
        $ export VERSION="|scala_binary_version|-${TAG}"
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)

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
The easiest way to get started is to download the most recent binary version from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
Download and extract it somewhere convenient:
=======
.. note::

  In the following examples, replace ``${TAG}`` with the corresponding GeoMesa version (e.g. |release_version|), and
  ``${VERSION}`` with the appropriate Scala plus GeoMesa versions (e.g. |scala_release_version|).

Extract it somewhere convenient:
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
Download and extract it somewhere convenient:
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f71fa3c0e1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
Download and extract it somewhere convenient:
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
Download and extract it somewhere convenient:
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa-${TAG}/geomesa-cassandra_${VERSION}-bin.tar.gz"
    $ tar xvf geomesa-cassandra_${VERSION}-bin.tar.gz
    $ cd geomesa-cassandra_${VERSION}
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt  logs/
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 16b2e83f22 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
    $ ls
    bin/  conf/  dist/  docs/  examples/  lib/  LICENSE.txt  logs/
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f71fa3c0e1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb4102515 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
>>>>>>> 90ec70f559 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
=======
>>>>>>> 16b2e83f2 (GEOMESA-3176 Docs - fix download links in install instructions)
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb652 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4cc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

.. _cassandra_install_source:

Building from Source
--------------------

GeoMesa Cassandra may also be built from source. For more information, refer to the instructions on
`GitHub <https://github.com/locationtech/geomesa#building-from-source>`__.
`GitHub <https://github.com/locationtech/geomesa#building-from-source>`__.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa Cassandra
distribution. If you have built from source, the distribution is created in the ``target`` directory of
``geomesa-cassandra/geomesa-cassandra-dist``.

.. _setting_up_cassandra_commandline:

Setting up the Cassandra Command Line Tools
-------------------------------------------

GeoMesa Cassandra comes with a set of command line tools for managing Cassandra features located in
``geomesa-cassandra_${VERSION}/bin/`` of the binary distribution.

.. note::

    You can configure environment variables and classpath settings in ``geomesa-cassandra_${VERSION}/conf/*-env.sh``.

.. note::

    ``geomesa-cassandra`` will read the ``$CASSANDRA_HOME`` and ``$HADOOP_HOME`` environment variables to load the
    appropriate JAR files for Cassandra and Hadoop. In addition, ``geomesa-cassandra`` will pull any
    additional JARs from the ``$GEOMESA_EXTRA_CLASSPATHS`` environment variable into the class path.
    Use the ``geomesa-cassandra classpath`` command in order to see what JARs are being used.

If you do not have a local Cassandra installation, the first time you run the tools it will prompt you to download
the necessary JARs. You may also do this manually using the scripts provided with the distribution:

.. code-block:: bash

    $ ./bin/install-dependencies.sh

Due to licensing restrictions, dependencies for shape file support must be separately installed.
Do this with the following command:

.. code-block:: bash

    $ ./bin/install-shapefile-support.sh

Run ``geomesa-cassandra`` without arguments to confirm that the tools work.

.. code::

    $ bin/geomesa-cassandra

The output should look like this::

    INFO  Usage: geomesa-cassandra [command] [command options]
      Commands:
      ...

.. _install_cassandra_geoserver:

Installing GeoMesa Cassandra in GeoServer
-----------------------------------------

.. warning::

    See :ref:`geoserver_versions` to ensure that GeoServer is compatible with your GeoMesa version.

The GeoMesa Cassandra distribution includes a GeoServer plugin for including
Cassandra data stores in GeoServer. The plugin files are in the
``dist/gs-plugins/geomesa-cassandra-gs-plugin_${VERSION}-install.tar.gz`` archive within the
GeoMesa Cassandra distribution directory.

To install the plugins, extract the archive and copy the contents to the ``WEB-INF/lib``
directory of your GeoServer installation::

.. code-block:: bash

    $ tar -xzvf \
      geomesa-cassandra_${VERSION}/dist/gs-plugins/geomesa-cassandra-gs-plugin_${VERSION}-install.tar.gz \
      -C /path/to/geoserver/webapps/geoserver/WEB-INF/lib


Next, install the JARs for Cassandra. By default, JARs will be downloaded from Maven central. You may
override this by setting the environment variable ``GEOMESA_MAVEN_URL``. If you do no have an internet connection
you can download the JARs manually.

Edit the file ``geomesa-cassandra_${VERSION}/conf/dependencies.sh`` to set the version of Cassandra
to match the target environment, and then run the script:

.. code-block:: bash

    $ ./bin/install-dependencies.sh /path/to/geoserver/webapps/geoserver/WEB-INF/lib

Restart GeoServer after the JARs are installed.
