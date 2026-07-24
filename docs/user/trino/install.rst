.. _trino_install:

Installing GeoMesa Trino
========================

.. note::

    GeoMesa currently supports Trino {{trino_supported_versions}}.

Installing from the Binary Distribution
---------------------------------------

GeoMesa Trino artifacts are available for download or can be built from source.
The easiest way to get started is to download the most recent binary version from `GitHub`__.

__ https://github.com/locationtech/geomesa/releases

Download and extract it somewhere convenient:

.. code-block:: bash

    # download and unpackage the most recent distribution:
    $ wget "https://github.com/locationtech/geomesa/releases/download/geomesa-{{release}}/geomesa-trino_{{scala_binary_version}}-{{release}}-bin.tar.gz"
    $ tar xvf geomesa-trino_{{scala_binary_version}}-{{release}}-bin.tar.gz
    $ cd geomesa-trino_{{scala_binary_version}}-{{release}}

.. note::

    The Trino plugin JAR only bundles support for the AWS S3 filesystem and Iceberg REST and AWS Glue catalogs. To use
    Snowflake, Azure, GCS, or other filesystems, build a custom plugin JAR from source with the necessary JARs
    added as dependencies.

Building from Source
--------------------

GeoMesa Trino may also be built from source. For more information, refer to the instructions on
`GitHub <https://github.com/locationtech/geomesa#building-from-source>`__.
The remainder of the instructions in this chapter assume the use of the binary GeoMesa Trino
distribution. If you have built from source, the distribution is created in the ``target`` directory of
``geomesa-trino/geomesa-trino-dist``.

.. _trino_plugin_install:

Deploying the Plugin into a Trino Cluster
-----------------------------------------

Install the Plugin JAR
^^^^^^^^^^^^^^^^^^^^^^

The ``geomesa-trino_{{scala_binary_version}}-{{release}}/dist/trino/`` directory contains the server-side
JAR that must be deployed into the Trino cluster.

The version of the plugin runtime JAR must match the version of the GeoMesa
data store client JAR (usually installed in GeoServer; see below). If not,
queries might not work correctly or at all.

Trino loads each subdirectory of its plugin directory (``/usr/lib/trino/plugin/`` in the standard layout) as an independent
plugin. Create a new subdirectory on every coordinator and worker and copy the plugin JAR into it:

.. code-block:: bash

    mkdir /usr/lib/trino/plugin/spatial-iceberg
    cp geomesa-trino-plugin-{{release}}.jar /usr/lib/trino/plugin/spatial-iceberg/

Define a Spatial Catalog
^^^^^^^^^^^^^^^^^^^^^^^^

Next, define one or more catalogs by creating a properties file, e.g. ``/etc/trino/catalog/spatial_iceberg.properties``.
Configure it the same as an `Iceberg connector <https://trino.io/docs/current/connector/iceberg.html#>`__, but set
``connector.name=spatial_iceberg``. GeoMesa supports additional catalog properties - see :ref:`trino_configuration` for details.

For example:

.. tabs::

    .. code-tab:: properties REST Catalog

        connector.name=spatial_iceberg
        iceberg.catalog.type=rest
        iceberg.rest-catalog.uri=http://<catalog-host>:8181
        iceberg.file-format=PARQUET
        fs.s3.enabled=true
        s3.endpoint=<s3-endpoint>
        s3.region=<region>
        s3.aws-access-key=<key>
        s3.aws-secret-key=<secret>

    .. code-tab:: properties Glue Catalog

        connector.name=spatial_iceberg
        iceberg.catalog.type=glue
        iceberg.file-format=PARQUET
        iceberg.metadata-cache.enabled=true
        hive.metastore.glue.region=<region>
        hive.metastore.glue.default-warehouse-dir=s3://<bucket>/<warehouse-prefix>
        fs.s3.enabled=true
        s3.region=<region>

Once the connectors are defined, restart all nodes in the cluster.

Verify Installation
^^^^^^^^^^^^^^^^^^^

The catalog can be verified with ``SHOW CATALOGS;`` in the Trino CLI. Spatial pruning can be verified using EXPLAIN checks
as outlined in :ref:`trino_verify_pruning`.

.. _setting_up_trino_commandline:

Setting up the Trino Command Line Tools
---------------------------------------

.. warning::

    To use the Trino data store with the command line tools, you need to install
    the Trino plugin first. See :ref:`trino_plugin_install`.

GeoMesa comes with a set of command line tools for managing Trino features located in
``geomesa-trino_{{scala_binary_version}}-{{release}}/bin/`` of the binary distribution.

GeoMesa requires ``java`` to be available on the default path.

Configuring the Classpath
^^^^^^^^^^^^^^^^^^^^^^^^^

GeoMesa provides the ability to add additional JARs to the classpath using the environmental variable
``$GEOMESA_EXTRA_CLASSPATHS``. GeoMesa will prepend the contents of this environmental variable  to the computed
classpath, giving it highest precedence in the classpath. Users can provide directories of jar files or individual
files using a colon (``:``) as a delimiter.

For logging, see :ref:`slf4j_configuration` for information about configuring the SLF4J implementation.

Use the ``geomesa-trino classpath`` command to print the final classpath that will be used when executing GeoMesa
commands.

Running Commands
^^^^^^^^^^^^^^^^

Test the command that invokes the GeoMesa Tools:

.. code-block:: bash

    $ geomesa-trino

The output should look like this::

    Usage: geomesa-trino [command] [command options]
      Commands:
      ...

For details on the available commands, see :ref:`trino_tools`.

.. _install_trino_geoserver:

Installing GeoMesa Trino in GeoServer
-------------------------------------

.. warning::

    See :ref:`geoserver_versions` to ensure that GeoServer is compatible with your GeoMesa version.

Installing GeoServer
^^^^^^^^^^^^^^^^^^^^

As described in :ref:`geomesa_and_geoserver`, GeoMesa implements a `GeoTools <https://geotools.org/>`_-compatible data store.
This makes it possible to use GeoMesa Trino as a data store in `GeoServer <https://geoserver.org/>`_. GeoServer's web site includes
`installation instructions for GeoServer`_.

.. _installation instructions for GeoServer: https://docs.geoserver.org/stable/en/user/installation/index.html

After GeoServer is running, you may optionally install the WPS plugin. The GeoServer WPS Plugin must match the
version of your GeoServer instance. The GeoServer website includes instructions for downloading
and installing `the WPS plugin`_.

.. _the WPS plugin: https://docs.geoserver.org/stable/en/user/services/wps/install.html

.. note::

    If using Tomcat as a web server, it will most likely be necessary to
    pass some custom options::

        export CATALINA_OPTS="-Xmx8g -XX:MaxPermSize=512M -Duser.timezone=UTC \
        -server -Djava.awt.headless=true"

    The value of ``-Xmx`` should be as large as your system will permit. Be sure to
    restart Tomcat for changes to take place.

Installing the GeoMesa Trino Data Store
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To install the GeoMesa data store, extract the contents of the
``geomesa-trino-gs-plugin_{{scala_binary_version}}-{{release}}-install.tar.gz`` file in ``geomesa-trino_{{scala_binary_version}}-{{release}}/dist/gs-plugins/``
in the binary distribution or ``geomesa-trino/geomesa-trino-gs-plugin/target/`` in the source
distribution into your GeoServer's ``lib`` directory:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-trino_{{scala_binary_version}}-{{release}}/dist/gs-plugins/geomesa-trino-gs-plugin_{{scala_binary_version}}-{{release}}-install.tar.gz \
      -C /path/to/geoserver/webapps/geoserver/WEB-INF/lib

Restart GeoServer after the JARs are installed.

Install GeoMesa Processes
^^^^^^^^^^^^^^^^^^^^^^^^^

GeoMesa provides some WPS processes, such as ``geomesa:Density`` which is used to generate heat maps. In order
to use these processes, install the GeoServer WPS plugin as described in the :ref:`geomesa_process_install` guide.

Upgrading
---------

To upgrade between minor releases of GeoMesa, the versions of all GeoMesa components
**must** match. This means that the version of the ``geomesa-trino-plugin``
JAR installed in the Trino cluster **must** match the version of the
``geomesa-plugin`` JARs installed in the ``WEB-INF/lib`` directory of GeoServer.

See :ref:`upgrade_guide` for more details on upgrading between versions.
