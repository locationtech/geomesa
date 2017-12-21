.. _lambda_tools:

Lambda Command-Line Tools
=========================

The GeoMesa Lambda distribution includes a set of command-line tools for feature
management, ingest, export and debugging.

To install the tools, see :ref:`setting_up_lambda_commandline`.

Once installed, the tools should be available through the command ``geomesa-lambda``::

    $ geomesa-lambda
    INFO  Usage: geomesa-lambda [command] [command options]
      Commands:
        ...

Commands that are common to multiple back ends are described in :doc:`/user/cli/index`. Features
persisted to Accumulo can be accessed through the :doc:`/user/accumulo/commandline`.

General Arguments
-----------------

The Lambda tools require all the same parameter as the :doc:`/user/accumulo/commandline`. In addition,
the connection to Kafka must be specified. This is done through ``--brokers`` (or ``-b``). The Zookeeper
server can be specified through ``--kafka-zookeepers``, or will use the Accumulo Zookeepers otherwise.
