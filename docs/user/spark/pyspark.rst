GeoMesa PySpark
---------------

GeoMesa provides support for working with data stored in Accumulo using the Spark Python API.

Prerequisites
^^^^^^^^^^^^^

* `Spark`_ |spark_version| should be installed.
* `Python`_ 2.7 or 3.x should be installed.
* `pip`_ or ``pip3`` should be installed.

Installation
^^^^^^^^^^^^

The ``geomesa_pyspark`` package is not available for download. Build the artifact locally with the profile
``-Ppython``. Then install using ``pip`` or ``pip3`` as below. You will also need to put the
geomesa-accumulo-spark-runtime JAR in a convenient location. This JAR bundles together the client libraries
for GeoMesa Spark.

.. code-block:: bash

    mvn clean install -Ppython
    pip3 install geomesa-spark/geomesa_pyspark/target/geomesa_pyspark-$VERSION.tar.gz
    cp  geomesa-accumulo/geomesa-accumulo-spark-runtime/target/geomesa-accumulo-spark-runtime_2.11-$VERSION.jar /path/to/

Using Geomesa PySpark
^^^^^^^^^^^^^^^^^^^^^

You may then access Spark using a Yarn master by default. Importantly, because of the way the ``geomesa_pyspark``
library interacts with the underlying Java libraries, you must set up the GeoMesa configuration before referencing
the ``pyspark`` library.

.. code-block:: python

    import geomesa_pyspark
    conf = geomesa_pyspark.configure(
        jars=['/path/to/geomesa-accumulo-spark-runtime_2.11-$VERSION.jar'],
        packages=['geomesa_pyspark','pytz'],
        spark_home='/path/to/spark/').\
        setAppName('MyTestApp')

    conf.get('spark.master')
    # u'yarn'

    from pyspark.sql import SparkSession

    spark = ( SparkSession
        .builder
        .config(conf=conf)
        .enableHiveSupport()
        .getOrCreate()
    )

At this point you are ready to create a dict of connection parameters to your Accumulo data store and get a spatial
data frame.

.. code-block:: python

    params = {
        "accumulo.instance.id": "myInstance",
        "accumulo.zookeepers": "zoo1,zoo2,zoo3",
        "accumulo.user": "user",
        "accumulo.password": "password",
        "accumulo.catalog": "myCatalog"
    }
    feature = "mySchema"
    df = ( spark
        .read
        .format("geomesa")
        .options(**params)
        .option("geomesa.feature", feature)
        .load()
    )

    df.createOrReplaceTempView("tbl")
    spark.sql("show tables").show()

    # Count features in a bounding box.
    spark.sql("""
    select count(*)
    from tbl
    where st_contains(st_makeBBOX(-72.0, 40.0, -71.0, 41.0), geom)
    """).show()

You can terminate the Spark job on YARN using ``spark.stop()``.

Jupyter
^^^^^^^

To use the ``geomesa_pyspark`` package within Jupyter, you only needs a Python2 or Python3 kernel, which is
provided by default. Substitute the appropriate Spark home and runtime JAR paths in the above code blocks. Be sure
the GeoMesa Accumulo client and server side versions match, as described in :doc:`/user/accumulo/install`.

.. _pip: https://packaging.python.org/tutorials/installing-packages/
.. _Python: https://www.python.org/
.. _Spark: http://spark.apache.org/
