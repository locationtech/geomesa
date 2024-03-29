GeoMesa Spark
=============

.. note::

    GeoMesa currently supports Spark |spark_supported_versions|.

GeoMesa Spark allows for execution of jobs on Apache Spark using data stored in GeoMesa,
other GeoTools ``DataStore``\ s, or files readable by the GeoMesa converter library.
The library allows creation of Spark ``RDD``\ s and ``DataFrame``\ s, writing of
Spark ``RDD``\ s and ``DataFrame``\ s to GeoMesa ``DataStore``\ s, and serialization
of ``SimpleFeature``\ s using Kryo.

.. toctree::
   :maxdepth: 1

   architecture
   spark_jts
   core
   providers
   sparksql
   sparksql_functions
   pyspark
   jupyter
   zeppelin
