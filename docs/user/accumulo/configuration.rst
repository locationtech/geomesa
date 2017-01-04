Configuration
-------------

GeoMesa Accumulo uses a site XML file to maintain system property configurations. This file can be found
at ``conf/geomesa-site.xml`` of the Accumulo distribution. The default settings for GeoMesa are
stored in ``conf/geomesa-site.xml.template``. Do not modify this file directly as it is never read;
instead copy the desired configurations into ``geomesa-site.xml``.

By default, system properties set through command line parameters will take precedence over this
configuration file. If you wish a configuration item to always take precedence, even over command
line parameters, change the ``<final>`` tag to true.

By default configuration properties with empty values will not be applied, you can change this
by marking a property as final.

Zookeeper session timeout
~~~~~~~~~~~~~~~~~~~~~~~~~

The `Zookeeper session timeout <http://accumulo.apache.org/1.6/accumulo_user_manual#_instance_zookeeper_timeout>`__
for the GeoMesa Accumulo data store is exposed as the Java system property ``instance.zookeeper.timeout``:

.. code-block:: bash

    export JAVA_OPTS="-Dinstance.zookeeper.timeout=10s"

To preserve configuration set this property in ``geomesa-site.xml``.