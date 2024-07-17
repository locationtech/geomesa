GeoMesa PySpark
===============

GeoMesa provides integration with the Spark Python API for accessing data in GeoMesa data stores.

Prerequisites
-------------

* `Spark`_ |spark_required_version| should be installed.
* `Python`_ 2.7 or 3.x should be installed.
* `pip`_ or ``pip3`` should be installed.
* `conda-pack`_ is optional.

Installation
------------

The ``geomesa_pyspark`` package is not available for download. Build the artifact locally with the profile
``-Ppython``. Then install using ``pip`` or ``pip3`` as below. You will also need an appropriate
``geomesa-spark-runtime`` JAR. We assume the use of Accumulo here, but you may alternatively use any of
the providers outlined in :ref:`spatial_rdd_providers`.

.. code-block:: bash

    mvn clean install -Ppython
    pip3 install geomesa-spark/geomesa_pyspark/target/geomesa_pyspark-$VERSION.tar.gz
    cp  geomesa-accumulo/geomesa-accumulo-spark-runtime-accumulo21/target/geomesa-accumulo-spark-runtime-accumulo21_${VERSION}.jar /path/to/

Alternatively, you can use ``conda-pack`` to bundle the dependencies for your project. This may be more appropriate if
you have additional dependencies.

.. code-block:: bash

    export ENV_NAME=geomesa-pyspark

    conda create --name $ENV_NAME -y python=3.7
    conda activate $ENV_NAME

    pip install geomesa-spark/geomesa_pyspark/target/geomesa_pyspark-$VERSION.tar.gz
    # Install additional dependencies using conda or pip here

    conda pack -o environment.tar.gz
    cp geomesa-accumulo/geomesa-accumulo-spark-runtime-accumulo21/target/geomesa-accumulo-spark-runtime-accumulo21_${VERSION}.jar /path/to/

.. warning::
    ``conda-pack`` currently has issues with Python 3.8, and ``pyspark`` has issues with Python 3.9, hence the explicit
    use of Python 3.7


Using GeoMesa PySpark
---------------------

You may then access Spark using a Yarn master by default. Importantly, because of the way the ``geomesa_pyspark``
library interacts with the underlying Java libraries, you must set up the GeoMesa configuration before referencing
the ``pyspark`` library.

.. code-block:: python

    import geomesa_pyspark
    conf = geomesa_pyspark.configure(
        jars=['/path/to/geomesa-accumulo-spark-runtime-accumulo21_${VERSION}.jar'],
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
    --jars /path/to/geomesa-accumulo-spark-runtime_${VERSION}.jar \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
    --master yarn --deploy-mode client --archives environment.tar.gz#environment

At this point you are ready to create a dict of connection parameters to your Accumulo data store and get a spatial
data frame.

.. code-block:: python

    params = {
        "accumulo.instance.name": "myInstance",
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

Using Geomesa UDFs in PySpark
-----------------------------

There are 3 different ways to use the Geomesa UDFs from PySpark: from the SQL API, from the Fluent API via SQL expressions, or from the Fluent API via Python wrappers.
These approaches are equivalent performance-wise, so choosing the best approach for your project comes down to preference.

1. Accessing the Geomesa UDFs from the SQL API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We can access the Geomesa UDFs via the SQL API by simply including the functions in our SQL expressions.

.. code-block:: python

    df.createOrReplaceTempView("tbl")

    spark.sql("""
    select count(*) from tbl
    where st_contains(st_makeBBOX(-72.0, 40.0, -71.0, 41.0), geom)
    """).show()

2. Accessing the Geomesa UDFs from the Fluent API via SQL Expressions 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We can also access the Geomesa UDFs from the Fluent API via the `pyspark.sql.functions` module. This module has an `expr` function that we can use to access the Geomesa UDFs.

.. code-block:: python

    import pyspark.sql.functions as F

    # add a new column
    df = df.withColumn("geom_wkt", F.expr("st_asText(geom)"))

    # filter using SQL where expression
    df = df.select("*").where("st_area(geom) > 0.001")

    df.show()

3. Accessing the Geomesa UDFs from the Fluent API via Python Wrappers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We also support using the Geomesa UDFs as standalone functions through the use of Python wrappers. The Python wrappers for the Geomesa UDFs run on the JVM and are faster than logically equivalent Python UDFs.

.. code-block:: python

    from geomesa_pyspark.scala.functions import st_asText, st_area
    
    df = df.withColumn("geom_wkt", st_asText("geom"))
    df = df.withColumn("geom_area", st_area("geom"))

    df.show()

Using Custom Scala UDFs from PySpark
------------------------------------

We provide some utility functions in `geomesa_pyspark` that allow you to use your own Scala UDFs as standalone functions from PySpark. The advantage here is that you can write your UDFs in java or scala (so they run on the JVM), but can be used naturally from PySpark as if it were part of the Fluent API. This gives us the ability to write and use performant UDFs from PySpark without having to rely on Python UDFs, which can often be prohibitively slow for larger datasets.

.. code-block:: python

    from functools import partial
    from geomesa_pyspark.scala.udf import build_scala_udf, scala_udf, ColumnOrName
    from pyspark import SparkContext
    from pyspark.sql.column import Column

    sc = SparkContext.getOrCreate()
    custom_udfs = sc._jvm.path.to.your.CustomUserDefinedFunctions

    # use the helper function for building your udf
    def my_scala_udf(col: ColumnOrName) -> Column:
        """helpful docstring that explains what col is"""
        return build_scala_udf(sc, custom_udfs.my_scala_udf)(col)

    # or alternatively, build it directly by partially applying the scala udf
    my_other_udf = partial(scala_udf, sc, custom_udfs.my_other_udf())

    df.withColumn("edited_field_1", my_scala_udf("field_1")).show()
    df.withColumn("edited_field_2", my_other_udf("field_2")).show()

Recall that these UDFs can actually take either a `pyspark.sql.column.Column` or the string name of the column we wish to operate on, so the following are equivalent:

.. code-block:: python

    # this is more readable
    df.withColumn("edited_field_1", my_scala_udf("field_1")).show()

    # but we can also do this
    df.withColumn("edited_field_1", my_scala_udf(col("field_1"))).show()

Jupyter
-------

To use the ``geomesa_pyspark`` package within Jupyter, you only needs a Python2 or Python3 kernel, which is
provided by default. Substitute the appropriate Spark home and runtime JAR paths in the above code blocks. Be sure
the GeoMesa Accumulo client and server side versions match, as described in :doc:`/user/accumulo/install`.

.. _pip: https://packaging.python.org/tutorials/installing-packages/
.. _Python: https://www.python.org/
.. _Spark: https://spark.apache.org/
.. _conda-pack: https://conda.github.io/conda-pack/
