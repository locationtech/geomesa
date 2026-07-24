.. _trino_tools:

Trino Command-Line Tools
========================

The GeoMesa Trino distribution includes a set of command-line tools for feature
management, export and debugging.

To install the tools, see :ref:`setting_up_trino_commandline`.

Once installed, the tools should be available through the command ``geomesa-trino``::

    $ geomesa-trino
    INFO  Usage: geomesa-trino [command] [command options]
      Commands:
        ...

Commands are described in :doc:`/user/cli/index`, but because Trino is a read-only store, only some of the common commands are
available.

General Arguments
-----------------

Most commands require you to specify the connection to Trino. This generally includes the host, port,
and schema. Specify the host with ``--host``, the port with ``--port``, and the schema with ``--schema``.
Additionally, the connection user can be specified with ``--user``, and the catalog secret with ``--secret``.

The ``--auths`` argument corresponds to the ``TrinoDataStore`` parameter ``geomesa.security.auths``. See
:ref:`data_security` for more information.
