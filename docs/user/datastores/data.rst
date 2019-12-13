.. _data_migration:

Moving and Migrating Data
=========================

If you want an offline copy of your data, or you want to move data between networks, you can
export compressed Avro files containing your simple features.

.. note::

    The following examples assume an Accumulo install. For other back-ends, the command
    will vary slightly. See :ref:`command_line_tools` for more details.

To do this using the command line tools, use the export command with the ``format`` and ``gzip`` options:

.. code-block:: bash

    $ geomesa-accumulo export ... -f mySft --format avro --gzip 6 -o myFeatures.avro

To re-import the data into another environment, you may use the ingest command. Because the Avro file
is self-describing, you do not need to specify any converter config or simple feature type definition:

.. code-block:: bash

    $ geomesa-accumulo ingest ... -f mySft myFeatures.avro

If your data is too large for a single file, you may run multiple exports and use CQL
filters to separate your data.

If the schema does not already exist in the destination cluster, it will be created with the latest index formats
available in GeoMesa, which may perform better. You could use this technique to migrate data between tables
in a single cluster, as a way to benefit from indexing improvements. See :ref:`index_versioning` for more information.
