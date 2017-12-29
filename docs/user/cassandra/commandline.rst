.. _cassandra_tools:

Cassandra Command-Line Tools
============================

The GeoMesa Cassandra distribution includes a set of command-line tools for feature
management, ingest, export and debugging.

To install the tools, see :ref:`setting_up_cassandra_commandline`.

Once installed, the tools should be available through the command ``geomesa-cassandra``::

    $ geomesa-cassandra
    INFO  Usage: geomesa-cassandra [command] [command options]
      Commands:
        ...

All Cassandra commands are described in the common tools chapter :doc:`/user/cli/index`.

General Arguments
-----------------

Most commands require you to specify the connection to Cassandra. This generally includes a contact point,
key space, username and password. Specify the contact point and key space with ``--contact-point`` and
``--key-space`` (or ``-P`` and ``-k``). Specify the username and password with ``--user`` and ``--password``
(or ``-u`` and ``-p``). In order to avoid plaintext passwords in the bash history
and process list, the password argument may be omitted, which will trigger a prompt for it later.
