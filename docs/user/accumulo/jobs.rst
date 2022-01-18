GeoMesa Jobs
============

This project (``geomesa-accumulo/geomesa-accumulo-jobs`` in the source distribution) contains Map-Reduce
jobs for maintaining GeoMesa Accumulo.

Building Instructions
---------------------

If you wish to build ``geomesa-accumulo-jobs`` separately, you can with Maven:

.. code-block:: shell

    geomesa-accumulo$ mvn clean install -pl geomesa-accumulo-jobs

GeoMesa Input and Output Formats
--------------------------------

GeoMesa provides input and output formats that can be used in Hadoop
map/reduce jobs. The input/output formats can be used directly in Scala,
or there are Java interfaces under the ``interop`` package.

There are sample jobs provided that can be used as templates for more
complex operations. These are:

::

    org.locationtech.geomesa.jobs.interop.mapreduce.FeatureCountJob
    org.locationtech.geomesa.jobs.interop.mapreduce.FeatureWriterJob

GeoMesaInputFormat
^^^^^^^^^^^^^^^^^^

The ``GeoMesaInputFormat`` can be used to get ``SimpleFeature``\ s into
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

Use the static ``configure`` method to set up your job. You need to
provide it with a map of connection parameters, which will be used to
retrieve the GeoTools ``DataStore``. Optionally, you can also configure
the BatchWriter configuration used to write data to Accumulo.

The key you output does not matter, and will be ignored. The value
should be a ``SimpleFeature`` that you wish to write. If the
``SimpleFeatureType`` associated with the ``SimpleFeature`` does not yet
exist in GeoMesa, it will be created for you. You may write different
``SimpleFeatureType``\ s, but note that they will all share a common
catalog table.

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
        org.locationtech.geomesa.jobs.index.AttributeIndexJob \
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
        org.locationtech.geomesa.jobs.index.SchemaCopyJob \
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

