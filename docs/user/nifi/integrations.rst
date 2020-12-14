Cloud Provider Integrations
---------------------------

AWS Credentials Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The GeoMesa HBase and FileSystem processors support AWS integration through a configurable
``AWSCredentialsProviderService``. This provides pluggable credentials for accessing S3, which
can be used as a backing object store for HBase and the FileSystem data store.

GetHDFS Processor with Azure Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is possible to use the `Hadoop Azure
Support <https://hadoop.apache.org/docs/current/hadoop-azure/index.html>`__
to access Azure Blob Storage using HDFS. You can leverage this
capability to have the GetHDFS processor pull data directly from the
Azure Blob store. However, due to how the GetHDFS processor was written,
the ``fs.defaultFS`` configuration property is always used when
accessing ``wasb://`` URIs. This means that the ``wasb://`` container
you want the GetHDFS processor to connect to must be hard coded in the
HDFS ``core-site.xml`` config. This causes two problems. Firstly, it
implies that we can only connect to one container in one account on
Azure. Secondly, it causes problems when using NiFi on a server that is
also running GeoMesa-Accumulo as the ``fs.defaultFS`` property needs to
be set to the proper HDFS master NameNode.

There are two ways to circumvent this problem. You can maintain a
``core-site.xml`` file for each container you want to access but this is
not easily scalable or maintainable in the long run. The better option
is to leave the default ``fs.defaultFS`` value in the HDFS
``core-site.xml`` file. We can then leverage the
``Hadoop Configuration Resources`` property in the GetHDFS processor.
Normally you would set the ``Hadoop Configuration Resources`` property
to the location of the ``core-site.xml`` and the ``hdfs-site.xml``
files. Instead we are going to create an additional file and include it
last in the path so that it overwrites the value of the ``fs.defaultFS``
when the configuration object is built. To do this simply create a new
xml file with an appropriate name (we suggest the name of the
container). Insert the ``fs.defaultFS`` property into the file and set
the value.

.. code-block:: xml

    <configuration>
        <property>
            <name>fs.defaultFS</name>
            <value>wasb://container@accountName.blob.core.windows.net/</value>
        </property>
    </configuration>
