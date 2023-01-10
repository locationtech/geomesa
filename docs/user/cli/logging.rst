

.. _slf4j_configuration:

Logging Configuration
=====================

Each GeoMesa binary distribution comes bundled with an SLF4J logging implementation in the ``lib`` directory. If
you already have an SLF4J implementation on your Java classpath you may see errors at runtime and will have to
exclude one of the JARs. This can be accomplished by simply deleting the bundled ``lib/slf4j-reload4j-1.7.36.jar``.

Note that if no SLF4J implementation is installed logging will not work, and you will see this error:

.. code::

    SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
    SLF4J: Defaulting to no-operation (NOP) logger implementation
    SLF4J: See https://www.slf4j.org/codes.html#StaticLoggerBinder for further details.

<<<<<<< HEAD
In this case you may download SLF4J `here <https://www.slf4j.org/download.html>`__. Extract
=======
In this case you may download SLF4J `here <http://www.slf4j.org/download.html>`__. Extract
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
``slf4j-reload4j-1.7.36.jar`` and place it in the ``lib`` directory of the binary distribution.
