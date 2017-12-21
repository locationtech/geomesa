.. _bigtable_tools:

Bigtable Command-Line Tools
===========================

The GeoMesa Bigtable distribution includes a set of command-line tools for feature
management, ingest, export and debugging.

To install the tools, see :ref:`install_bigtable_tools`.

Once installed, the tools should be available through the command ``geomesa-bigtable``::

    $ geomesa-bigtable
    INFO  Usage: geomesa-bigtable [command] [command options]
      Commands:
        ...

All Bigtable commands are described in the common tools chapter :doc:`/user/cli/index`.

General Arguments
-----------------

The Bigtable tools commands do not require connection arguments; instead they rely on an appropriate
``hbase-site.xml`` to be available on the classpath, as described in :ref:`install_bigtable_tools`.
