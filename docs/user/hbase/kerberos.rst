                             Kerberos
========

GeoMesa includes initial support for HBase clusters which are authenticated using Kerberos. Currently, keytabs are supported.

Kerberos functionality should be configured by appending the following properties to ``hbase-site.xml``:

- ``hbase.geomesa.keytab``
- ``hbase.geomesa.principal``

All applications will require access to ``hbase-site.xml`` on their classpath in order to obtain the correct configuration.

Note that Kerberos support is only available in HBase 1.1.2 or newer.

Development & Testing
---------------------

GeoMesa Kerberos support was developed against Hortonworks Data Platform 2.6 authenticating against an MIT KDC.
So far, it has been tested in a limited development environment with Hortonworks Data Platform 2.6 on a single node.


.. note::

    To use geomesa in a kerberized environment add the following properties
    .. code::
    <property>
         <name>hbase.geomesa.principal</name>
         <value>hbaseGeomesa/_HOST@machineName</value>
    </property>

    <property>
         <name>hbase.geomesa.keytab</name>
         <value>/etc/security/keytabs/hbase.geomesa.keytab</value>
    </property>