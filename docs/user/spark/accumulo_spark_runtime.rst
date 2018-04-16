Using the GeoMesa Accumulo Spark Runtime
========================================

The ``geomesa-accumulo-spark-runtime`` module (found in the ``geomesa-accumulo`` directory in the GeoMesa source
distribution) provides a shaded JAR with all of the dependencies for Spark and Spark SQL analysis for data
stored in GeoMesa Accumulo, including the GeoMesa Accumulo data store, the GeoMesa Spark and GeoMesa Spark SQL
modules, and the ``AccumuloSpatialRDDProvider``.

.. note::

    In the example below, ``$VERSION`` = |release|.

The shaded JAR can either be built from source (see :ref:`building_from_source`), or is included in
the ``dist/spark`` directory of the GeoMesa Accumulo binary distribution. This JAR may be passed as an argument
to the Spark command line tools, or to `Jupyter`_ running the `Toree`_ kernel, without having to also include the
other dependencies requred to run GeoMesa or Accumulo.

.. _Jupyter: http://jupyter.org/

.. _Toree: https://toree.apache.org/

For example, the JAR can be used with the ``--jars`` switch of the ``spark-shell`` command:

.. code::

    $ spark-shell --jars /path/to/geomesa-accumulo-spark-runtime_2.11-$VERSION.jar

You should see a logging message loading the shaded JAR as the Spark console comes up:

.. code::

    ...
    17/01/16 14:31:21 INFO SparkContext: Added JAR file:/path/to/geomesa-accumulo-spark-runtime_2.11-$VERSION.jar at http://192.168.3.14:40775/jars/geomesa-accumulo-spark-runtime_2.11-$VERSION.jar with timestamp 1484595081362
    ...
