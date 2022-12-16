Installing GeoMesa FileSystem
=============================

.. note::

    The examples below expect a version to be set in the environment:

    .. parsed-literal::

        $ export TAG="|release_version|"
        $ export VERSION="|scala_binary_version|-${TAG}" # note: |scala_binary_version| is the Scala build version

Installing from the Binary Distribution
---------------------------------------

GeoMesa FileSystem artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

Download and extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa-${TAG}/geomesa-fs_${VERSION}-bin.tar.gz"
    $ tar xvf geomesa-fs_${VERSION}-bin.tar.gz
    $ cd geomesa-fs_${VERSION}

.. _fsds_install_source:

Building from Source
--------------------

GeoMesa FileSystem may also be built from source. For more information refer to :ref:`building_from_source`
in the developer manual, or to the ``README.md`` file in the the source distribution.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa
distribution. If you have built from source, the distribution is created in the ``target`` directory of
``geomesa-fs/geomesa-fs-dist``.

More information about developing with GeoMesa may be found in the :doc:`/developer/index`.

.. _setting_up_fsds_commandline:

Setting up the FileSystem Command Line Tools
--------------------------------------------

The FileSystem command line tools require Hadoop to run. If ``HADOOP_HOME`` is defined, or ``hadoop`` is available
on the path, the tools will use the local Hadoop installation. Otherwise, when first run they will prompt to download
the necessary JARs. Environment variables can be specified in ``conf/*-env.sh`` and dependency versions can be
specified in ``conf/dependencies.sh``.

Note that you will need the proper Yarn/Hadoop environment configured if you would like to run a distributed ingest
job.

If you are using a service such as Amazon Elastic MapReduce (EMR) or have a distribution of Apache Hadoop, Cloudera, or
Hortonworks installed you can likely run something like this to configure hadoop for the tools:

.. code-block:: bash

    # These will be specific to your Hadoop environment
    $ . /etc/hadoop/conf/hadoop-env.sh
    $ . /etc/hadoop/conf/yarn-env.sh
    $ export HADOOP_CONF_DIR=/etc/hadoop/conf

After installing the tarball you should be able to run the ``geomesa-fs`` command like this:

.. code-block:: bash

    $ cd $GEOMESA_FS_HOME
    $ bin/geomesa-fs

The output should look like this::

    INFO  Usage: geomesa-fs [command] [command options]
      Commands:
        ...

.. _install_fsds_geoserver:

Installing GeoMesa FileSystem in GeoServer
------------------------------------------

.. warning::

    See :ref:`geoserver_versions` to ensure that GeoServer is compatible with your GeoMesa version.

To install the GeoMesa data store, extract the contents of the
``geomesa-fs-gs-plugin_${VERSION}-install.tar.gz`` file in ``geomesa-fs_${VERSION}/dist/gs-plugins/``
in the binary distribution or ``geomesa-fs/geomesa-fs-gs-plugin/target/`` in the source
distribution into your GeoServer's ``lib`` directory:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-fs_${VERSION}/dist/gs-plugins/geomesa-fs-gs-plugin_${VERSION}-install.tar.gz \
      -C /path/to/geoserver/webapps/geoserver/WEB-INF/lib

Next, install the JARs for Hadoop. By default, JARs will be downloaded from Maven central. You may
override this by setting the environment variable ``GEOMESA_MAVEN_URL``. If you do not have an internet connection
you can download the JARs manually.

Edit the file ``geomesa-fs_${VERSION}/conf/dependencies.sh`` to set the version of Hadoop
to match the target environment, and then run the script:

.. code-block:: bash

    $ ./bin/install-dependencies.sh /path/to/geoserver/webapps/geoserver/WEB-INF/lib

The FileSystem data store requires the configuration file ``core-site.xml`` to be on the classpath. This can
be accomplished by placing the file in ``geoserver/WEB-INF/classes`` (you should make the directory if it
doesn't exist). Utilizing a symbolic link will be useful here so any changes are reflected in GeoServer.

.. code-block:: bash

    $ ln -s /path/to/core-site.xml /path/to/geoserver/WEB-INF/classes/core-site.xml

Restart GeoServer after the JARs are installed.

GeoMesa Process
^^^^^^^^^^^^^^^

GeoMesa provides some WPS processes, such as ``geomesa:Density`` which is used to generate heat maps. In order
to use these processes, install the GeoServer WPS plugin as described in :ref:`geomesa_process`.
