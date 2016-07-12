GeoMesa Spark
=============

GeoMesa Spark allows for execution of jobs on Apache Spark using data stored in GeoMesa. Through an API provided in the
geomesa-compute module, we allow creation of Spark RDDs, writing of Spark RDDs to GeoMesa data stores, and serialization
of simple features using Kryo.

Usage
-----

To use GeoMesa with Spark, the executors must know how to serialize and deserialize Simple Features. There are two ways
to accomplish this.

Restart the Spark Context
^^^^^^^^^^^^^^^^^^^^^^^^^

One option is initialize a new Spark Context with the desired data store or SimpleFeatureType.
This involves calling ``GeoMesaSpark.init``, which will take an existing Spark Configuration, and return a new one
that is set to use our GeoMesaSparkKryoRegistrator to serialize Simple Features of the provided types. This return
value must be used to initialize a new Spark Context as ``init`` will also set system properties for all executors
such that they can serialize features of those types.
Multiple data stores are able to be initialized by continually passing the resulting configuration into multiple calls
to ``init`` and restarting the context using the final result.

.. code-block:: scala

    // Call initialize, retrieving a new spark config
    val newConfig = GeoMesaSpark.init(new SparkConf(), dataStore)
    // Stop all other running Spark Contexts
    Option(sc).foreach(_.stop())
    // Initialize a new one with the desired config
    val sc = new SparkContext(newConfig)

Broadcast Simple Feature Types
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Alternatively, if a restart of the Spark Context is undesirable, one is able to register classes directly into the Kryo
Registrator. To do this, take advantage of the fact that basic Spark configuration values can be set in ``spark-defaults.conf``
In this pattern, a call to ``GeoMesaSpark.register`` will register the Simple Feature Types of a provided data store,
skipping the need use shared system properties that require a restart. The caveat, however, is that before serialization,
the SimpleFeatureType encodings must be sent to the executors via a Spark Broadcast and then used to create the corresponding
types in each executor's registrator.

.. code-block:: scala

    // Register the sfts of a given data store
    GeoMesaSpark.register(dataStore)
    // Broadcast sft encodings to executors
    val broadcastedSfts = sc.broadcast(sfts.map{sft => (name, SimpleFeatureTypes.encodeType(sft)})
    // Populate the type cache on each partition
    someRdd.foreachPartition { iter =>
        broadcastedSfts.foreach { case (name, spec) =>
            val sft = SimpleFeatureTypes.createType(name, spec)
            GeoMesaSparkKryoRegistrator.putType(sft)
        }
    }


Connect to Data Stores
^^^^^^^^^^^^^^^^^^^^^^

GeoMesa Spark further provides functionality to read a data store schema into a Spark RDD. To do this, it is best to
place connection credentials into ``spark-defaults.conf`` in ``${SPARK_HOME}/conf``. With this, set up the connection
parameters.

.. code-block:: scala

    val params = Map(
      "instanceId" -> "mycloud",
      "zookeepers" -> "zoo1,zoo2,zoo3",
      "user"       -> sc.getConf.get("spark.credentials.ds.username"),
      "password"   -> sc.getConf.get("spark.credentials.ds.password"),
      "tableName"  -> "mytable")

And create the RDD

.. code-block:: scala

    val rdd = GeoMesaSpark.rdd(new Configuration, sc, params, query)


Examples
--------

For a complete example of analysis with Spark, see :doc:`../tutorials/spark`

