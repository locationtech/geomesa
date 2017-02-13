Bootstrapping GeoMesa HBase on AWS S3
==========================================================

GeoMesa can be run on top of HBase using S3 as the underlying storage engine.  This mode of running GeoMesa is cost-effective as one sizes the database cluster for the compute and memory requirements, not the storage requirements.  The following guide describes how to bootstrap GeoMesa in this manner.  This guide assumes you have an Amazon Web Services account already provisioned as well as an IAM key pair.  To set up the AWS command line tools, follow the instructions found in the AWS `online documentation <http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html>`_.

.. _Amazon Web Services: https://aws.amazon.com/

.. _Amazon ElasticMapReduce: https://aws.amazon.com/emr/

Bootstrap an EMR cluster with HBase
-----------------------------------

First, you will need to configure an S3 bucket for use by HBase.  Create a json file called ``geomesa-hbase-on-s3.json`` with the following content.  Make sure to replace ``geomesa-root-path`` with a unique root directory for HBase.

.. code-block:: javascript

  [
     {
       "Classification": "hbase-site",
       "Properties": {
         "hbase.rootdir": "s3://<geomesa-root-path>/root"}
     },
     {
       "Classification": "hbase",
       "Properties": {
       "hbase.emr.storageMode":"s3"
       }
     }
  ]

Then, ensure that the bucket exists.

.. code-block:: shell
   
   $ aws s3 mb s3://<geomesa-root-path>/root


Then, Use the following command to bootstrap an EMR cluster with HBase.  You will need to change the ``KeyName`` to the IAM key pair you intend to use for this cluster and ``SubnetId`` to the id of the subnet if that key is associated with a specific subnet.  You can also edit the instance types to a size appropriate for your use case.  Specify the appropriate path to the json file you created in the last step.

.. note::

  In the code below, ``$VERSION`` = |release|

.. code-block:: shell

  $ aws emr create-cluster                                \                                                                                                                                     
    --name "GeoMesa HBase on S3"                          \
    --release-label emr-5.2.0                             \
    --output text                                         \
    --use-default-roles                                   \
    --ec2-attributes KeyName=__KEY_NAME__,SubnetId=__SUBNET_ID__      \
    --applications Name=Hadoop Name=Zookeeper Name=Spark Name=HBase   \
    --instance-groups                                                 \
      Name=Master,InstanceCount=1,InstanceGroupType=MASTER,InstanceType=m4.2xlarge \
      Name=Workers,InstanceCount=3,InstanceGroupType=CORE,InstanceType=m4.xlarge   \
    --configurations file:///path/to/geomesa-hbase-on-s3.json

After executing that command, you can monitor the state of the EMR bootstrap process
by going to the `Management Console <https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-list>`_.  Find the name (as specified in the ``aws emr`` command) of the cluster and click through to its details page.  Under the **Hardware** section, you can find the master node and its IP address.  Copy the IP address and then run the following command.

.. code-block:: shell

   $ ssh -i /path/to/key ec2-user@<ip_address>

This should log you into the master node of the EMR cluster you just
started. Now, download the GeoMesa HBase distribution.

.. code-block:: shell

   $ wget https://repo.locationtech.org/content/repositories/geomesa-releases/org/locationtech/geomesa/geomesa-hbase_2.11/${VERSION}/geomesa-hbase_2.11-${VERSION}-bin.tar.gz -o /tmp/geomesa-hbase_2.11-${VERSION}-bin.tar.gz
   $ cd /opt
   $ sudo tar zxvf /tmp/geomesa-hbase_2.11-${VERSION}-bin.tar.gz

Then, bootstrap GeoMesa on HBase on S3 by executing the provided script

.. code-block:: shell

   $ sudo /opt/geomesa-hbase_2.11-${VERSION}/bin/bootstrap-geomesa-hbase.sh ${VERSION}

Now, log out and back in and your environment will be set up appropriately.
 

Ingest Public GDELT data
------------------------

GeoMesa ships with predefined data models for many open spatio-temporal data sets such as GDELT.  To ingest the most recent 7 days of `GDELT
<http://www.gdeltproject.org>`_ from Amazon's public S3 bucket:

.. code-block:: shell

    $ mkdir gdelt
    $ cd gdelt
    $ seq 7 -1 1 | xargs -n 1 -I{} sh -c "date -d'{} days ago' +%Y%m%d" | xargs -n 1 -I{} aws s3 cp  s3://gdelt-open-data/events/{}.export.csv .
    $ geomesa-hbase ingest -c geomesa.gdelt -C gdelt -f gdelt -s gdelt \*.csv

You can then query the data using the GeoMesa command line export tool.

.. code-block:: shell

    $ geomesa-hbase export -c geomesa.gdelt -f gdelt -m 100

Setup GeoMesa and SparkSQL
-----------------------------------------------------------------

To start executing SQL queries using Spark over your GeoMesa on HBase on S3 cluster, set up the following variable, replacing ``VERSION`` with the appropriate version of GeoMesa.

.. code-block:: shell
    
    $ JARS=file:///usr/lib/hbase/hbase-common.jar,file:///usr/lib/hbase/hbase-client.jar,file:///usr/lib/hbase/hbase-protocol.jar,file:///usr/lib/hbase/hbase-server.jar,file:///opt/geomesa/dist/spark/geomesa-hbase-spark-runtime_2.11-${VERSION}.jar,file:///usr/lib/hbase/conf/hbase-site.xml

Then, start up the Spark shell

.. code-block:: shell

    $ spark-shell --jars $JARS

Within the Spark shell, you can connect to GDELT and issues some queries.

.. code-block:: scala

   scala> val df = spark.read.format("geomesa").option("bigtable.table.name", "geomesa.gdelt").option("geomesa.feature", "gdelt").load()

   scala> df.createOrReplaceTempView("gdelt")

   scala> spark.sql("select globalEventId,geom,dtg from gdelt limit 5").show() 


