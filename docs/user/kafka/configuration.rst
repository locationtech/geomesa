Configuration
=============

Use of the Kafka data store does not require server-side configuration,
however, a ``geomesa-site.xml`` configuration file is available for
setting system properties.

This file can be found at ``conf/geomesa-site.xml`` of the Kafka
distribution. The default settings for GeoMesa are stored in
``conf/geomesa-site.xml.template``. Do not modify this file directly as it
is never read; instead, copy the desired configurations into
``geomesa-site.xml``.

By default, command line parameters will take precedence over this
configuration file. If you wish a configuration item to always take
precedence, even over command line parameters, change the ``<final>``
tag to true.

By default, configuration properties with empty values will not be
applied, you can change this by marking a property as final.