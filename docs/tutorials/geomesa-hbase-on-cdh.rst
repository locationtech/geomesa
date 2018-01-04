Deploying GeoMesa HBase on Cloudera CDH
=======================================

GeoMesa can be run on top of HBase using S3 as the underlying storage engine.  This mode of running GeoMesa is
cost-effective as one sizes the database cluster for the compute and memory requirements, not the storage requirements.


.. code-block:: shell
   
    $ aws s3 mb s3://<bucket-name>
    $ aws s3api put-object --bucket <bucket-name> --key hbase-root/

You should now be able to list the contents of your bucket:

- item 1
- item 2


