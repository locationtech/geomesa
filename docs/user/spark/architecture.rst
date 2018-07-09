Architecture
------------

GeoMesa Spark provides capabilities to run geospatial analysis jobs on
the distributed, large-scale data processing engine `Apache Spark`_.
It provides interfaces for Spark to ingest and analyze geospatial data
stored in GeoMesa data stores.

GeoMesa provides Spark integration at several different levels. At the lowest level
is the ``geomesa-spark-jts`` module (see :doc:`./spark_jts`), which contains user-defined spatial types
and functions. This module can easily be included in other projects that want to
work with geometries in Spark, as it only depends on the JTS library.

Next, the ``geomesa-spark-core`` module (see :doc:`./core`) is an extension for Spark that takes
`GeoTools`_ ``Query`` objects as input and produces resilient distributed datasets
(``RDD``\ s) containing serialized versions of SimpleFeatures. Multiple
backends that target different types of feature stores are available,
including ones for Accumulo, HBase, FileSystem, Kudu, files readable by the :ref:`converters` library,
and any generic GeoTools ``DataStore``\ s.

The ``geomesa-spark-sql`` module (see :doc:`./sparksql`) builds on top of the core module
to convert between ``RDD``\ s and ``DataFrame``\ s. GeoMesa SparkSQL pushes down
filtering logic from SQL queries and converts them into GeoTools ``Query`` objects,
which are then passed to the ``GeoMesaSpark`` object provided by GeoMesa Spark Core.

Finally, bindings are provided for integration with the Spark Python API. See :doc:`./pyspark` for details.

.. image:: /user/_static/img/geomesa-spark-stack.png
   :align: center

A stack composed of a distributed data store such as Accumulo, GeoMesa,
the GeoMesa Spark libraries, Spark, and the `Jupyter`_ interactive notebook application
(see above) provides a complete large-scale geospatial data analysis platform.

See :doc:`/tutorials/spark` for a tutorial on analyzing data with GeoMesa Spark.

.. _Apache Spark: https://spark.apache.org/

.. _Jupyter: http://jupyter.org/
