Architecture
------------

GeoMesa Spark provides capabilities to run geospatial analysis jobs on
the distributed, large-scala data processing engine `Apache Spark`_. It provides
interfaces to ingest and analyze geospatial data stored in GeoMesa Accumulo
and other data stores.

GeoMesa Spark is divided into two modules.

GeoMesa :doc:`./core` (``geomesa-spark-core``) is an extension for Spark that takes
`GeoTools`_ ``Query`` objects as input and produces resilient distributed datasets
(``RDD``\ s) containing serialized versions of geometry objects. Multiple
backends that target different types of feature stores are available,
including ones for GeoMesa Accumulo, other GeoTools ``DataStore``\ s, or files
readable by the :doc:`/user/convert` library.

GeoMesa :doc:`./sparksql` (``geomesa-spark-sql``), in turn, stacks on GeoMesa Spark
Core to convert between ``RDD``\ s and ``DataFrame``\ s. GeoMesa SparkSQL pushes down
filtering logic from SQL queries and converts them into GeoTools ``Query`` objects,
which are then passed to the ``GeoMesaSpark`` object provided by GeoMesa Spark Core.
GeoMesa SparkSQL also provides a number of user-defined types and functions for
working with geometry objects.

.. image:: /user/_static/img/geomesa-spark-stack.png
   :align: center

A stack composed of a distributed data store such as Accumulo, GeoMesa,
the GeoMesa Spark libraries, Spark, and the `Jupyter`_ interactive notebook application
(see above) provides a complete large-scale geospatial data analysis platform.

See :doc:`/tutorials/spark` for a tutorial on analyzing data with GeoMesa Spark.

.. _Apache Spark: https://spark.apache.org/

.. _Jupyter: http://jupyter.org/
