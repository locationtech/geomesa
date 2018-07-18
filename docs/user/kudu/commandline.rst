.. _kudu_tools:

Using the Kudu Command-Line Tools
=================================

The GeoMesa Kudu distribution includes a set of command-line tools for feature
management, ingest, export and debugging.

To install the tools, see :ref:`setting_up_kudu_commandline`.

Once installed, the tools should be available through the command ``geomesa-kudu``::

    $ geomesa-kudu
    INFO  Usage: geomesa-kudu [command] [command options]
      Commands:
        ...

Kudu supports all the common commands described in :doc:`/user/cli/index`.

General Arguments
-----------------

Most commands require that you specify the Kudu master, using ``--master`` or ``-M``.
