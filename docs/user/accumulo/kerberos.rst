Kerberos
========

GeoMesa includes initial support for Accumulo clusters which are authenticated using Kerberos. Currently keytabs
(and not cached TGTs) are supported, apart from initial setup which requires a cached TGT, usually obtained using ``kinit``.

Kerberos functionality should be used as follows:

#. ``setup_namespace.sh`` should be called with the ``-t`` flag to use a cached TGT.
#. ``geomesa`` command line tools should be used with the ``--keytab`` parameter. Ensure ``ACCUMULO_HOME`` and ``HADOOP_HOME`` are both set.
#. Programmatic access via the GeoTools API should specify the ``accumulo.keytab.path`` parameter.
#. The GeoServer store should specify the ``accumulo.keytab.path`` parameter. Ensure ``core-site.xml`` is accessible to GeoServer e.g. in the ``webapps/geoserver/WEB-INF/classes/`` directory.

Note that Kerberos support is only available in Accumulo 1.7.0 or newer.

Development & Testing
---------------------

GeoMesa Kerberos support was developed against Hortonworks Data Platform 2.5 authenticating against an MIT KDC as described here_.
It has been tested in a limited production environment with Hortonworks Data Platform 2.5 authenticating against a `Red Hat Identity Management server`_.

.. _here: https://docs.hortonworks.com/HDPDocuments/Ambari-2.4.2.0/bk_ambari-security/content/ch_configuring_amb_hdp_for_kerberos.html

.. _`Red Hat Identity Management server`: https://access.redhat.com/products/identity-management