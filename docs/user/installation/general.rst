General Configuration
=====================

The following configuration options apply to all GeoMesa installations.

Configuration Properties
------------------------

GeoMesa uses a site XML file to maintain system property configurations. When run, GeoMesa will look
on the classpath for a file named ``geomesa-site.xml``. These settings can also be controlled
by system properties.

To configure a GeoMesa tools distribution, place ``geomesa-site.xml`` in the ``conf`` folder.
For GeoServer, place the file under ``geoserver/WEB-INF/classes``.

Each tools tools distribution contains a template file with the default settings at
``conf/geomesa-site.xml.template``. Do not modify this file directly as it is never read;
instead copy the desired configurations into ``geomesa-site.xml``.

By default, system properties set through command line parameters will take precedence over this
configuration file. If you wish a configuration item to always take precedence, even over command
line parameters, change the ``<final>`` tag to true.

By default configuration properties with empty values will not be applied, you can change this
by marking a property as final.

.. _slf4j_configuration:

SLF4J Configuration
-------------------

Each GeoMesa Tools distribution comes bundled by default with an SLF4J implementation that is installed to the
``lib`` directory named ``slf4j-log4j12-1.7.5.jar``. If you already have an SLF4J implementation
installed on your Java classpath you may see errors at runtime and will have to exclude one of the JARs. This can be
done by simply deleting the bundled ``slf4j-log4j12-1.7.5.jar``.

Note that if no SLF4J implementation is installed you will see this error:

.. code::

    SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
    SLF4J: Defaulting to no-operation (NOP) logger implementation
    SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.

In this case you may download SLF4J `here <http://www.slf4j.org/download.html>`__. Extract
``slf4j-log4j12-1.7.7.jar`` and place it in the ``lib`` directory of the binary distribution.
If this conflicts with another SLF4J implementation, you may need to remove it from the ``lib`` directory.


Security Concerns
-----------------

Apache Commons Collections
^^^^^^^^^^^^^^^^^^^^^^^^^^

Version 3.2.1 and earlier of the Apache Commons Collections library have a CVSS 10.0 vulnerability.  Read more `here
<https://commons.apache.org/proper/commons-collections/security-reports.html>`__.

Accumulo 1.6.5+ and GeoServer 2.8.3+/2.9.0+ include the patched JAR. Users on older versions should install
the newer jar manually.
