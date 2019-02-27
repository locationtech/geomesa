GeoMesa Avro Binary Format Example
==================================

This tutorial shows how to use Java to serialize and deserialize GeoMesa data to and from the Avro format.

About this Tutorial
-------------------

In the spirit of keeping things simple, the code in this tutorial only
does a few small things:

1. Queries GeoMesa for SimpleFeatures
2. Writes the data to an Avro file
3. Reads the data back out of the Avro file

Choice of Backing Storage
-------------------------

This tutorial will work with several different back-ends. For simplicity, the rest of the tutorial will assume
the use of HBase. Alternatively, you may use Accumulo, Cassandra, Redis, or the GeoMesa FileSystem DataStore. If not
using HBase, the commands in the rest of the tutorial will vary slightly.

Prerequisites
-------------

Before you begin, you must have the following:

-  `Java <http://java.oracle.com/>`__ JDK 1.8
-  Apache `Maven <http://maven.apache.org/>`__ |maven_version|
-  a GitHub client
-  Completion of the GeoMesa quick start for your choice of back end

.. warning::

    This tutorial will use the data you wrote in the initial quick start, so make sure you complete that first.

Download and Build the Tutorial
-------------------------------

Pick a reasonable directory on your machine, and run:

.. code-block:: bash

    $ git clone https://github.com/geomesa/geomesa-tutorials.git
    $ cd geomesa-tutorials

.. warning::

    Make sure that you download or checkout the version of the tutorials project that corresponds to
    your GeoMesa version. See :ref:`tutorial_versions` for more details.

To ensure that the tutorial works with your environment, modify the ``pom.xml``
to set the appropriate versions for HBase, Hadoop, etc.

For ease of use, the project builds a bundled artifact that contains all the required
dependencies in a single JAR. To build, run:

.. code-block:: bash

    $ mvn clean install -pl geomesa-tutorials-hbase/geomesa-tutorials-hbase-avro -am

.. note::

    The module name will vary depending on choice of back end.

Run the Tutorial
----------------

On the command-line, run:

.. code-block:: bash

    $ java -cp geomesa-tutorials-hbase/geomesa-tutorials-hbase-avro/target/geomesa-tutorials-hbase-avro-$VERSION.jar \
        org.geomesa.example.hbase.avro.HBaseAvroTutorial \
        --hbase.zookeepers <zookeepers>                  \
        --hbase.catalog <table>

where you provide the following arguments:

-  ``<zookeepers>`` the HBase Zookeeper quorum. If you installed HBase in stand-alone mode,
   this will be ``localhost``. Note that for most use cases, it is preferable to put the
   ``hbase-site.xml`` from your cluster on the GeoMesa classpath instead of specifying Zookeepers.
-  ``<table>`` the name of the table that holds your quick-start data

.. note::

    The path, class name, and required arguments will vary depending on choice of back end.

The code will query GeoMesa for data, write it to a byte array, and then read it back out:

.. code-block:: none

    Loading datastore

    Querying data store and writing features to Avro binary format
    Wrote 2356 features as 72680 bytes
    Reading features back from Avro binary format
    01 719024896=719024896|UNITED STATES|USA|SENATE||042|2|1|2|2|Texas, United States|US|2017-12-31T00:00:00.000Z|POINT (-97.6475 31.106)
    02 719024892=719024892|UNITED STATES|USA|DEPUTY||010|4|1|4|3|Abbeville, South Carolina, United States|US|2017-12-31T00:00:00.000Z|POINT (-82.379 34.1782)
    03 719024891=719024891|UNITED STATES|USA|||010|2|1|2|3|Ninety Six, South Carolina, United States|US|2017-12-31T00:00:00.000Z|POINT (-82.024 34.1751)
    04 719024889=719024889|SENATE||UNITED STATES|USA|043|2|1|2|3|Washington, District of Columbia, United States|US|2017-12-31T00:00:00.000Z|POINT (-77.0364 38.8951)
    05 719024890=719024890|NIGERIA|NGA|PRESIDENT||020|2|1|2|4|Ibadan, Oyo, Nigeria|NI|2017-12-31T00:00:00.000Z|POINT (3.89639 7.38778)
    06 719025151=719025151|ARGENTINE|ARG|DIOCESE||010|3|1|3|4|Corrientes, Corrientes, Argentina|AR|2018-01-01T00:00:00.000Z|POINT (-58.8341 -27.4806)
    07 719027031=719027031|UNITED STATES|USA|||193|4|1|4|1|Brazil|BR|2018-01-01T00:00:00.000Z|POINT (-55 -10)
    08 719025141=719025141|AFRICA|AFR|DIPLOMAT||040|4|1|4|1|South Africa|SF|2018-01-01T00:00:00.000Z|POINT (26 -30)
    09 719025751=719025751|GOVERNMENT||||071|10|1|10|4|Maputo, Maputo, Mozambique|MZ|2018-01-01T00:00:00.000Z|POINT (32.5892 -25.9653)
    10 719025053=719025053|||NIGERIAN|NGA|100|1|1|1|1|Angola|AO|2018-01-01T00:00:00.000Z|POINT (18.5 -12.5)

    Read back 2356 total features

    Done

Looking at the Code
-------------------

The source code is meant to be accessible for this tutorial. The main logic is contained in
the generic ``org.geomesa.example.avro.GeoMesaAvroTutorial`` in the ``geomesa-tutorials-common`` module,
which is datastore agnostic. The encoding happens in the ``queryFeatures`` method:

.. code-block:: java

    // some code omitted for clarity

    try (AvroDataFileWriter writer = new AvroDataFileWriter(out, sft, -1)) {
        while (reader.hasNext()) {
            writer.append(reader.next());
            n++;
        }
        writer.flush();
    }

    try (AvroDataFileReader reader = new AvroDataFileReader(new ByteArrayInputStream(bytes))) {
        while (reader.hasNext()) {
            SimpleFeature feature = reader.next();
        }
    }
