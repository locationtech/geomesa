.. _hbase_tools:

HBase Command-Line Tools
========================

The GeoMesa HBase distribution includes a set of command-line tools for feature
management, ingest, export and debugging.

To install the tools, see :ref:`setting_up_hbase_commandline`.

Once installed, the tools should be available through the command ``geomesa-hbase``::

    $ geomesa-hbase
    INFO  Usage: geomesa-hbase [command] [command options]
      Commands:
        ...

All HBase commands are described in the common tools chapter :doc:`/user/cli/index`.

General Arguments
-----------------

The HBase tools commands do not require connection arguments; instead they rely on an appropriate
``hbase-site.xml`` to be available on the classpath, as described in :ref:`setting_up_hbase_commandline`.
