GeoMesa Spark
=============

GeoMesa Spark allows for execution of jobs on Apache Spark using data stored in GeoMesa,
other GeoTools ``DataStore``\ s, or files readable by the GeoMesa converter library.
The library allows creation of Spark ``RDD``\ s and ``DataFrame``\ s, writing of
Spark ``RDD``\ s and ``DataFrame``\ s to GeoMesa Accumulo and other GeoTools ``DataStore``\ s, and serialization of ``SimpleFeature``\ s using Kryo.

The current version of GeoMesa Spark supports Apache Spark 2.0.

.. toctree::
   :maxdepth: 1

   architecture
   core
   sparksql
   sparksql_functions
   accumulo_spark_runtime
   jupyter
   zeppelin
