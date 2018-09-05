GeoMesa Spark SQL on Google Cloud Dataproc
==========================================

GeoMesa can run Spark SQL with Bigtable as the underlying datastore.  In order to set up Spark SQL,
you will need to launch a Google Cloud Dataproc cluster.  First, you will need to install the Google
Cloud SDK command line tools.  Instructions for doing so can be found `here <https://cloud.google.com/sdk/downloads>`_.
Ensure that you have installed all the appropriate components for working with GCP Bigtable and GCP
Dataproc.

.. code-block:: shell

   $ gcloud components install cbt
   $ gcloud components install alpha
   $ gcloud components install beta

Then, provision a new Bigtable instance.

.. code-block:: shell

   $ gcloud beta bigtable instances create geomesa --cluster geomesa --cluster-zone us-east1-b --cluster-num-nodes 3 --description GeoMesa

And provision a GCP Dataproc cluster.

.. code-block:: shell

   $ gcloud dataproc clusters create geomesa

Once you have a GCP Dataproc cluster up and running, you can scp the GeoMesa Bigtable distribution to the master node.  You can
determine the name of the master node by using the following command line.

.. code-block:: shell

   $ gcloud compute instances list

Find the master node instance and then scp the distro as follows.

.. code-block:: shell

   $ gcloud beta compute scp --zone $ZONEID  ~/.m2/repository/org/locationtech/geomesa/geomesa-bigtable-dist_2.11/$VERSION/geomesa-bigtable_2.11-$VERSION-bin.tar.gz <masterhost>:~/

Log in to the master node using gcloud ssh as follows.

.. code-block:: shell

   $ gcloud compute --project $PROJECTID ssh --zone $ZONEID $MASTER


Now, configure the installation by unpacking the tarball, editing the hbase-site.xml appropriately, and including the hbase-site.xml in the 
spark runtime jar.

.. code-block:: shell

   $ tar zxvf geomesa-bigtable_2.11-$VERSION-bin.tar.gz
   $ ln -s geomesa-bigtable_2.11-$VERSION geomesa
   $ export PATH=$PATH:~/geomesa/bin
   $ export HADOOP_HOME=/usr/lib/hadoop
   $ export HBASE_HOME=/usr/lib/hbase
   $ vi geomesa/conf/hbase-site.xml
   $ cp geomesa/conf/hbase-site.xml geomesa/dist/spark/
   $ cd geomesa/dist/spark/
   $ jar uvf geomesa-bigtable-spark-runtime_2.11-$VERSION hbase-site.xml

Download sample GDELT data and ingest it as follows.

.. code-block:: shell

   $ geomesa-bigtable ingest -t 8 -c geomesa.gdelt -s gdelt -C gdelt \*.csv

Now, you can run a spark shell and execute Spark SQL over your GeoMesa on Bigtable instance.  Set the version to your installed version of GeoMesa.

.. code-block:: shell

   $ spark-shell --num-executors 4 --master yarn --jars file://$HOME/geomesa/dist/spark/geomesa-bigtable-spark-runtime_2.11-$VERSION.jar,file://$HOME/geomesa/lib/bigtable-hbase-1.2-0.9.4.jar,file://$HOME/geomesa/lib/netty-tcnative-boringssl-static-1.1.33.Fork19.jar

From the Spark shell prompt.

.. code-block:: shell

   scala> val df = spark.read.format("geomesa").option("bigtable.catalog", "geomesa.gdelt").option("geomesa.feature", "gdelt").load()
   scala> df.createOrReplaceTempView("gdelt")
   scala> spark.sql("SELECT actor1Name,actor2Name,geom,dtg FROM gdelt WHERE st_contains(st_geomFromWKT('POLYGON((-80 35,-70 35,-70 40,-80 40,-80 35))'),geom)").show()


