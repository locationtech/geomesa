Installation and Configuration
==============================

This chapter describes how to install GeoMesa on a Linux system.
This includes installation of the supported data stores and
installation and configuration of the GeoServer plugins.

.. toctree::

   installation/versions
   installation/accumulo
   installation/kafka
   installation/hbase
   installation/bigtable
   installation/cassandra

.. _slf4j_configuration:

SLF4J Configuration
-------------------

Each GeoMesa Tools distribution comes bundled by default with an SLF4J implementation that is installed to the
``lib`` directory named ``slf4j-log4j12-1.7.5.jar``. If you already have an SLF4J implementation
installed on your Java classpath you may see errors at runtime and will have to exclude one of the JARs. This can be
done by simply renaming the bundled ``slf4j-log4j12-1.7.5.jar`` file to ``slf4j-log4j12-1.7.5.jar.exclude``.

Note that if no SLF4J implementation is installed you will see this error:

.. code::

    SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
    SLF4J: Defaulting to no-operation (NOP) logger implementation
    SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.

In this case you may download SLF4J from http://www.slf4j.org/download.html. Extract
``slf4j-log4j12-1.7.7.jar`` and place it in the ``lib/common`` directory of the binary distribution.
If this conflicts with another SLF4J implementation, you may need to remove it from the ``lib/common`` directory.

Configuration
-------------

Multiple GeoMesa distributions use a site XML file to maintain system property configurations. This file can be found
at ``conf/geomesa-site.xml`` of each distribution. The default settings for GeoMesa are
stored in ``conf/geomesa-site.xml.template``. Do not modify this file directly as it is never read;
instead copy the desired configurations into ``geomesa-site.xml``.

By default, system properties set through command line parameters will take precedence over this
configuration file. If you wish a configuration item to always take precedence, even over command
line parameters, change the ``<final>`` tag to true.

By default configuration properties with empty values will not be applied, you can change this
by marking a property as final.

Security Concerns
-----------------

Apache Commons Collections
^^^^^^^^^^^^^^^^^^^^^^^^^^

Version 3.2.1 and earlier of the Apache Commons Collections library have a CVSS 10.0 vulnerability.  Read more `here
<https://commons.apache.org/proper/commons-collections/security-reports.html>`__.

Note that Accumulo 1.6.5 is the first version of Accumulo which addresses this security concern.
Fixes for the GeoServer project will be available in versions 2.8.3+ and 2.9.0+.
