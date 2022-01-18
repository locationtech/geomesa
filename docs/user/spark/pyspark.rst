GeoMesa PySpark
---------------

GeoMesa provides integration with the Spark Python API for accessing data in GeoMesa data stores.

Prerequisites
^^^^^^^^^^^^^

* `Spark`_ |spark_required_version| should be installed.
* `Python`_ 2.7 or 3.x should be installed.
* `pip`_ or ``pip3`` should be installed.
* `conda-pack`_ is optional.

Installation
^^^^^^^^^^^^

The ``geomesa_pyspark`` package is not available for download. Build the artifact locally with the profile
``-Ppython``. Then install using ``pip`` or ``pip3`` as below. You will also need an appropriate
``geomesa-spark-runtime`` JAR. We assume the use of Accumulo here, but you may alternatively use any of
the providers outlined in :ref:`spatial_rdd_providers`.

.. code-block:: bash

    mvn clean install -Ppython
    pip3 install geomesa-spark/geomesa_pyspark/target/geomesa_pyspark-$VERSION.tar.gz
    cp  geomesa-accumulo/geomesa-accumulo-spark-runtime-accumulo2/target/geomesa-accumulo-spark-runtime-accumulo2_2.11-$VERSION.jar /path/to/

Alternatively, you can use ``conda-pack`` to bundle the dependencies for your project. This may be more appropriate if
you have additional dependencies.

.. code-block:: bash

    export ENV_NAME=geomesa-pyspark

    conda create --name $ENV_NAME -y python=3.7
    conda activate $ENV_NAME

    pip install geomesa-spark/geomesa_pyspark/target/geomesa_pyspark-$VERSION.tar.gz
    # Install additional dependencies using conda or pip here

    conda pack -o environment.tar.gz
    cp geomesa-accumulo/geomesa-accumulo-spark-runtime-accumulo2/target/geomesa-accumulo-spark-runtime-accumulo2_2.11-$VERSION.jar /path/to/

.. warning::
    ``conda-pack`` currently has issues with Python 3.8, and ``pyspark`` has issues with Python 3.9, hence the explicit
    use of Python 3.7


Using GeoMesa PySpark
^^^^^^^^^^^^^^^^^^^^^

You may then access Spark using a Yarn master by default. Importantly, because of the way the ``geomesa_pyspark``
library interacts with the underlying Java libraries, you must set up the GeoMesa configuration before referencing
the ``pyspark`` library.

.. code-block:: python

    import geomesa_pyspark
    conf = geomesa_pyspark.configure(
        jars=['/path/to/geomesa-accumulo-spark-runtime-accumulo2_2.11-$VERSION.jar'],
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

Alternatively, if you used ``conda-pack`` then you do not need to set up the GeoMesa configuration as above, but you
must start ``pyspark`` or your application as follows, updating paths as required:

.. code-block:: bash

    PYSPARK_DRIVER_PYTHON=/opt/anaconda3/envs/$ENV_NAME/bin/python PYSPARK_PYTHON=./environment/bin/python pyspark \
    --jars /path/to/geomesa-accumulo-spark-runtime_2.11-$VERSION.jar \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
    --master yarn --deploy-mode client --archives environment.tar.gz#environment

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

GeoMesa PySpark can also be used in the absence of a GeoMesa data store.  Registering user-defined types and functions
can be done manually by invoking ``geomesa_pyspark.init_sql()`` on the Spark session object:

.. code-block:: python

    geomesa_pyspark.init_sql(spark)


You can terminate the Spark job on YARN using ``spark.stop()``.

Jupyter
^^^^^^^

To use the ``geomesa_pyspark`` package within Jupyter, you only needs a Python2 or Python3 kernel, which is
provided by default. Substitute the appropriate Spark home and runtime JAR paths in the above code blocks. Be sure
the GeoMesa Accumulo client and server side versions match, as described in :doc:`/user/accumulo/install`.

.. _pip: https://packaging.python.org/tutorials/installing-packages/
.. _Python: https://www.python.org/
.. _Spark: http://spark.apache.org/
.. _conda-pack: https://conda.github.io/conda-pack/
