GeoMesa Jobs
============

This project (``geomesa-accumulo/geomesa-accumulo-jobs`` in the source distribution) contains Map-Reduce
jobs for maintaining GeoMesa Accumulo.

Building Instructions
---------------------

If you wish to build ``geomesa-accumulo-jobs`` separately, you can with Maven:

.. code-block:: shell

    $ mvn clean install -pl geomesa-accumulo/geomesa-accumulo-jobs -am

GeoMesa Input and Output Formats
--------------------------------

GeoMesa provides input and output formats that can be used in Hadoop
map/reduce jobs. The input/output formats can be used directly in Scala,
or there are Java interfaces under the ``interop`` package.

There are sample jobs provided that can be used as templates for more
complex operations. These are::

    org.locationtech.geomesa.accumulo.jobs.mapreduce.interop.FeatureCountJob
    org.locationtech.geomesa.accumulo.jobs.mapreduce.interop.FeatureWriterJob

<<<<<<< HEAD
GeoMesaAccumuloInputFormat
^^^^^^^^^^^^^^^^^^^^^^^^^^
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 96d5d442fa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0104fb37e4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
    org.locationtech.geomesa.jobs.interop.mapreduce.FeatureCountJob
    org.locationtech.geomesa.jobs.interop.mapreduce.FeatureWriterJob
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
    org.locationtech.geomesa.jobs.interop.mapreduce.FeatureCountJob
    org.locationtech.geomesa.jobs.interop.mapreduce.FeatureWriterJob
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
=======
    org.locationtech.geomesa.jobs.interop.mapreduce.FeatureCountJob
    org.locationtech.geomesa.jobs.interop.mapreduce.FeatureWriterJob
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 59a1fbb96e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    org.locationtech.geomesa.jobs.interop.mapreduce.FeatureCountJob
    org.locationtech.geomesa.jobs.interop.mapreduce.FeatureWriterJob
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 96d5d442fa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0104fb37e4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
    org.locationtech.geomesa.jobs.interop.mapreduce.FeatureCountJob
    org.locationtech.geomesa.jobs.interop.mapreduce.FeatureWriterJob
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 3d5144418e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))

The ``GeoMesaAccumuloInputFormat`` can be used to get ``SimpleFeature``\ s into
your jobs directly from GeoMesa.

Use the static ``configure`` method to set up your job. You need to
provide it with a map of connection parameters, which will be used to
retrieve the GeoTools DataStore. You also need to provide a feature type
name. Optionally, you can provide a CQL filter, which will be used to
select a subset of features in your store.

The key provided to your mapper with be a ``Text`` with the
``SimpleFeature`` ID. The value will be the ``SimpleFeature``.

GeoMesaOutputFormat
^^^^^^^^^^^^^^^^^^^

The ``GeoMesaOutputFormat`` can be used to write ``SimpleFeature``\ s
back into GeoMesa.

Use the static ``setOutput`` method to set up your job. You need to
provide it with a map of connection parameters, which will be used to
retrieve the GeoTools ``DataStore``.

The key you output does not matter, and will be ignored. The value
should be a ``SimpleFeature`` that you wish to write. If the
``SimpleFeatureType`` associated with the ``SimpleFeature`` does not yet
exist in GeoMesa, it will be created for you. You may write different
``SimpleFeatureType``\ s, in a single job, if desired.

Map/Reduce Jobs
---------------

The following instructions require that you use the ``-libjars`` argument to ensure the correct JARs
are available on the distributed classpath.

.. note::

  In the following examples, replace ``${VERSION}`` with the appropriate Scala plus GeoMesa versions
  (e.g. |scala_release_version|).

.. _attribute_indexing_job:

Attribute Indexing
^^^^^^^^^^^^^^^^^^

GeoMesa provides indexing on attributes to improve certain queries. You
can indicate attributes that should be indexed when you create your
schema (simple feature type). If you decide later on that you would like
to index additional attributes, you can use the attribute indexing job.
You only need to run this job once; the job will create attribute indices
for each attribute listed in ``--geomesa.index.attributes``.

The job can be invoked through Yarn as follows:

.. code-block:: shell

    geomesa-accumulo$ yarn jar geomesa-accumulo-jobs/target/geomesa-accumulo-jobs_${VERSION}.jar \
<<<<<<< HEAD
        org.locationtech.geomesa.accumulo.jobs.index.AttributeIndexJob \
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 96d5d442fa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0104fb37e4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
        org.locationtech.geomesa.jobs.index.AttributeIndexJob \
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        org.locationtech.geomesa.jobs.index.AttributeIndexJob \
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        org.locationtech.geomesa.jobs.index.AttributeIndexJob \
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0104fb37e4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        org.locationtech.geomesa.jobs.index.AttributeIndexJob \
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
=======
        org.locationtech.geomesa.jobs.index.AttributeIndexJob \
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 59a1fbb96e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 96d5d442fa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0104fb37e4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        org.locationtech.geomesa.jobs.index.AttributeIndexJob \
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 3d5144418e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
        --geomesa.input.instanceId <instance> \
        --geomesa.input.zookeepers <zookeepers> \
        --geomesa.input.user <user> \
        --geomesa.input.password <pwd> \
        --geomesa.input.tableName <catalog-table> \
        --geomesa.input.feature <feature> \
        --geomesa.index.coverage <full|join> \ # optional attribute
        --geomesa.index.attributes <attributes to index - space separated>

.. note::

    You will also need to include an extensive ``-libjars`` argument with all dependent JARs.

.. _update_index_format_job:

Updating Existing Data to the Latest Index Format
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The indexing in GeoMesa is constantly being improved. We strive to maintain
backwards compatibility, but old data can't always take advantage of the
improvements we make. However, old data can be updated through the
``SchemaCopyJob``. This will copy it to a new table (or feature name),
rewriting all the data using the latest codebase. Once the data is
updated, you can drop the old tables and rename the new tables back to
the original names.

The job can be invoked through Yarn as follows (JAR version may vary
slightly):

.. code-block:: shell

    geomesa-accumulo$ yarn jar geomesa-accumulo-jobs/target/geomesa-accumulo-jobs_${VERSION}.jar \
<<<<<<< HEAD
        org.locationtech.geomesa.accumulo.jobs.index.SchemaCopyJob \
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 96d5d442fa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0104fb37e4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
        org.locationtech.geomesa.jobs.index.SchemaCopyJob \
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        org.locationtech.geomesa.jobs.index.SchemaCopyJob \
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        org.locationtech.geomesa.jobs.index.SchemaCopyJob \
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 12e3a588fc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0104fb37e4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        org.locationtech.geomesa.jobs.index.SchemaCopyJob \
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f0b9bd8121 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
=======
        org.locationtech.geomesa.jobs.index.SchemaCopyJob \
<<<<<<< HEAD
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 59a1fbb96e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 810876750d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 96d5d442fa (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a52e4429be (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0104fb37e4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
        org.locationtech.geomesa.jobs.index.SchemaCopyJob \
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 3d5144418e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
        --geomesa.input.instanceId <instance> \
        --geomesa.output.instanceId <instance> \
        --geomesa.input.zookeepers <zookeepers> \
        --geomesa.output.zookeepers <zookeepers> \
        --geomesa.input.user <user> \
        --geomesa.output.user <user> \
        --geomesa.input.password <pwd> \
        --geomesa.output.password <pwd> \
        --geomesa.input.tableName <catalog-table> \
        --geomesa.output.tableName <new-catalog-table> \
        --geomesa.input.feature <feature> \
        --geomesa.output.feature <feature> \
        --geomesa.input.cql <options cql filter for input features>

.. note::

    You will also need to include an extensive ``-libjars`` argument with all dependent JARs.

