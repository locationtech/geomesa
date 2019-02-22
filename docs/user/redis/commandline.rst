.. _redis_tools:

Using the Redis Command-Line Tools
==================================

The GeoMesa Redis distribution includes a set of command-line tools for feature
management, ingest, export and debugging.

To install the tools, see :ref:`setting_up_redis_commandline`.

Once installed, the tools should be available through the command ``geomesa-redis``::

    $ geomesa-redis
    INFO  Usage: geomesa-redis [command] [command options]
      Commands:
        ...

Redis supports all the common commands described in :doc:`/user/cli/index`.

General Arguments
-----------------

Most commands require that you specify the Redis URL, using ``--url`` or ``-u``.
