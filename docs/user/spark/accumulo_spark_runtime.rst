Using the GeoMesa Accumulo Spark Runtime
========================================

The ``geomesa-accumulo-spark-runtime`` module (found in the ``geomesa-accumulo`` directory in the GeoMesa source distribution) provides a shaded JAR with all of the dependencies for Spark and Spark SQL analysis for data stored in GeoMesa Accumulo, including the GeoMesa Accumulo data store, the GeoMesa Spark and GeoMesa Spark SQL modules, and the ``AccumuloSpatialRDDProvider``.

.. note::

    In the example below, ``$VERSION`` = |release|.

The shaded JAR can either be built from source (see :ref:`building_from_source`), or is included in the ``dist/spark`` directory of the GeoMesa Accumulo binary distribution.
The JAR can be used with the ``--jars`` switch of the ``spark-shell`` command:

.. code::

    $ spark-shell --jars /path/to/geomesa-accumulo-spark-runtime_2.11-$VERSION.jar
    ...
    Welcome to
          ____              __
         / __/__  ___ _____/ /__
        _\ \/ _ \/ _ `/ __/  '_/
       /___/ .__/\_,_/_/ /_/\_\   version 1.6.3
          /_/

    Using Scala version 2.10.5 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_65)
    Type in expressions to have them evaluated.
    Type :help for more information.
    ...
    17/01/16 14:31:21 INFO SparkContext: Added JAR file:/path/to/geomesa-accumulo-spark-runtime_2.11-$VERSION.jar at http://192.168.3.14:40775/jars/geomesa-accumulo-spark-runtime_2.11-$VERSION.jar with timestamp 1484595081362
    ...
    17/01/16 14:31:21 INFO SparkILoop: Created spark context..
    Spark context available as sc.
    17/01/16 14:31:22 INFO SparkILoop: Created sql context..
    SQL context available as sqlContext.

    scala>
