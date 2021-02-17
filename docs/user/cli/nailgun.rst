.. _nailgun_server:

Nailgun Server
==============

GeoMesa can use a persistent server to eliminate the overhead of bootstrapping a JVM for each command, which can
make commands much faster to start. The server must be enabled by setting the environment variable
``GEOMESA_NG_ENABLED=true``.

.. warning::

  The Nailgun server provides no security, and should only be used in a trusted environment. By default
  it will bind to all host addresses, allowing unauthenticated connections from any client on the same network.

When enabled, the Nailgun server will start automatically as needed, and will terminate after a period of inactivity
(by default one hour). While it is running, the server will not pick up any classpath or environment changes.
The :ref:`ng_command` subset of commands can be used to start, stop, and query the server status.

The following environment variables control the behavior of the server, and can be configured in
``conf/geomesa-env.sh``:

.. code::

  GEOMESA_NG_ENABLED # enable the Nailgun server
  GEOMESA_NG_SERVER # the host to use to connect to the server, or to bind the server on startup
  GEOMESA_NG_PORT # the port to use to connect to the server, or to bind teh server on startup
  GEOMESA_NG_TIMEOUT # client heartbeat timeout
  GEOMESA_NG_IDLE # amount of time before the server will terminate due to inactivity
  GEOMESA_NG_POOL_SIZE # number of threads in the server available for running commands

Note that it is possible to run the Nailgun server on a different host than the client.
