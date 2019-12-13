Kerberos
========

GeoMesa includes initial support for HBase clusters which are authenticated using Kerberos. Currently, keytabs are supported.

Kerberos functionality should be configured by appending the following properties to ``hbase-site.xml``:

 * ``hbase.geomesa.keytab``
 * ``hbase.geomesa.principal``

All applications will require access to ``hbase-site.xml`` on their classpath in order to obtain the correct configuration.

Note that Kerberos support is only available in HBase 1.1.2 or newer.

Development & Testing
---------------------

GeoMesa Kerberos support was developed against Hortonworks Data Platform 2.6 authenticating against an MIT KDC.
So far, it has been tested in a limited development environment with Hortonworks Data Platform 2.6 on a single node.


.. note::

    To use GeoMesa in a Kerberized environment add the following properties to ``hbase-site.xml``:

    .. code::

        <property>
             <name>hbase.geomesa.principal</name>
             <value>hbaseGeomesa/_HOST@machineName</value>
        </property>

        <property>
             <name>hbase.geomesa.keytab</name>
             <value>/etc/security/keytabs/hbase.geomesa.keytab</value>
        </property>

Managing Hadoop and HBase configurations on the classpath
---------------------------------------------------------

In order to setup the GeoMesa command line tools, create symlinks of the Hadoop configuration files to ``$GEOMESA_HOME/conf/``.
Here is an example command to help do this:

    $ for i in $(ls /usr/hdp/current/hadoop-client/conf); do ln -s /usr/hdp/current/hadoop-client/conf/$i $GEOMESA_HOME/conf/; done
    $ ln -s  /usr/hdp/current/hbase-client/conf/hbase-site.xml $GEOMESA_HOME/conf/

You can verify that the GeoMesa HBase command line tools are working by ingest a small sample file.

For GeoServer, similarly, the above configuration files will need to copied or symlinked to ``geoserver/WEB-INF/classes/``.

    
Enabling Kerberos on HDP
------------------------
To enable kerberos on a HDP cluster you can either
 * do it all manually (not recommended)
 * use ambari as outlined in https://docs.hortonworks.com/HDPDocuments/Ambari-2.2.0.0/bk_Ambari_Security_Guide/content/ch_configuring_amb_hdp_for_kerberos.html
 * or deploy a kerberos enabled Ambari blueprint https://cwiki.apache.org/confluence/display/AMBARI/Blueprints like

.. code::

    {
      [
        {
          "kerberos-env": {
            "properties_attributes" : { },
            "properties" : {
              "realm" : "myOrg.com",
              "kdc_type" : "mit-kdc",
              "kdc_hosts" : "kdc.company.com",
              "admin_server_host" : "kdx.company.com"
            }
          }
        },
        {
          "krb5-conf": {
            "properties_attributes" : { },
            "properties" : {
              "domains" : "",
              "manage_krb5_conf" : "false"
            }
          }
        },
      ],
      "host_groups" : [
        {
          "name" : "host_group_1",
          "configurations" : [ ],
          "default_password": "hadoop",
          "components" : [
            { "name" : "INFRA_SOLR"             , "provision_action" : "INSTALL_AND_START" },
            ......
            { "name" : "ZOOKEEPER_CLIENT"       , "provision_action" : "INSTALL_AND_START" }
          ],
          "cardinality" : "1"
        }
      ],
      "Blueprints" : {
        "blueprint_name" : "hdp-2.6-sandbox",
        "stack_name" : "HDP",
        "stack_version" : "2.6",
        "security" : {
          "type" : "KERBEROS"
        }
      }
    }
