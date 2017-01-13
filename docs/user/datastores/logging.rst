.. _slf4j_configuration:

Logging Configuration
=====================

Each GeoMesa Tools distribution comes bundled by default with an SLF4J logging implementation that is installed
to the ``lib`` directory named ``slf4j-log4j12-1.7.5.jar``. If you already have an SLF4J implementation
installed on your Java classpath you may see errors at runtime and will have to exclude one of the JARs. This
can be accomplished by simply deleting the bundled ``slf4j-log4j12-1.7.5.jar``.

Note that if no SLF4J implementation is installed you will see this error:

.. code::

    SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
    SLF4J: Defaulting to no-operation (NOP) logger implementation
    SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.

In this case you may download SLF4J `here <http://www.slf4j.org/download.html>`__. Extract
``slf4j-log4j12-1.7.5.jar`` and place it in the ``lib`` directory of the binary distribution.
If this conflicts with another SLF4J implementation, you may need to remove it from the ``lib`` directory.
