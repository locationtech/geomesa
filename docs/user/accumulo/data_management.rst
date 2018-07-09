Data Management
===============

GeoMesa provides many ways to optimize your data storage. You can add additional indices to speed up
certain queries, disable indices to speed up ingestion, pre-split tables for optimal data
distribution and migrate data between tables or environments.

.. _accumulo_attribute_indices:

Accumulo Attribute Indices
--------------------------

See :ref:`attribute_indices` for an overview of attribute indices. The Accumulo data store extends the
normal attribute indices with an additional 'join' format that stores less data.

Join Indices
^^^^^^^^^^^^

Join indices store a reduced subset of data in the index - just the feature ID, the default date
and the default geometry. To answer most queries, a join against the record index is required
to retrieve the full simple features - hence the name join index. Joining against the record
table is slow when returning many results, and should generally be avoided except for small queries.

GeoMesa will avoid joining against the record table if it is possible to answer
a query with only the data in the join index. In general, this means that the query is only
returning the properties for the default date, default geometry and the attribute being queried.
In addition, any CQL filters must only operate on those three attributes as well.

To enable a join index, the keyword ``join`` may be used in place of ``true`` when specifying an
attribute index in the ``SimpleFeatureType``.

Full Indices
^^^^^^^^^^^^

Full indices store the full simple feature. This takes up the most space on disk, but allows for any query to
be answered without joining against the record table. This is the only option for non-Accumulo data stores.
To use a full index, the keyword ``full`` or ``true`` may be used when specifying an attribute
index in the ``SimpleFeatureType``.

.. _logical_timestamps:

Accumulo Logical Timestamps
---------------------------

By default, GeoMesa index tables are created using Accumulo's logical time. This ensures that updates to a given
simple feature will be ordered correctly, however it obscures the actual insert time for the underlying data
row. For advanced use cases, standard system time can be used instead of logical time. To disble logical
time, add the following user data hint to the simple feature type before calling ``createSchema``:

.. code-block:: java

    // append the hints to the end of the string, separated by a semi-colon
    String spec = "name:String,dtg:Date,*geom:Point:srid=4326;geomesa.logical.time='false'";
    SimpleFeatureType sft = SimpleFeatureTypes.createType("mySft", spec);

.. warning::

    If table sharing is enabled, then the index tables may have already been created with logical
    time enabled. If this is the case, then disabling it will have no effect. To ensure the
    tables are created correctly, you may wish to disable table sharing by adding the user data string
    ``geomesa.table.sharing='false'``

Table Sharing
-------------

By default, multiple ``SimpleFeatureType``\ s in a single catalog table will share the same physical Accumulo
index tables. This will reduce the total number of Accumulo tables created. However, it may complicate
administrator tasks, such as by making it impossible to set different tablet split sizes for different feature types.
In addition, deleting a schema will generally be slower, as it requires deleting each data row, instead of
just dropping the entire table.

Table sharing can be disabled by setting the user data ``geomesa.table.sharing`` to false:

.. code-block:: java

    // disable table sharing for this feature type
    sft.getUserData().put("geomesa.table.sharing", "false");

See :ref:`set_sft_options` for more details on how to set user data values.

.. _index_upgrades:

Upgrading Existing Indices
--------------------------

GeoMesa often makes updates to indexing formats to improve query and write performance. However,
the index format for a given schema is fixed when it is first created. Updating GeoMesa versions
will provide bug fixes and new features, but will not update existing data to new index formats.

The exact version of an index used for each schema can be read from the ``SimpleFeatureType`` user data,
or by simple examining the name of the index tables created by GeoMesa. See below for a description of
current index versions.

Using the GeoMesa command line tools, you can add or update an index to a newer version using ``add-index``.
For example, you could add the XZ3 index to replace the Z3 index for a feature type with non-point geometries.
The command will populate the new index using a distributed job. For large data sets, you can choose to
only populate features matching a CQL filter (e.g. the last month), or choose to not populate any
data. The update is seamless, and clients can continue to query and ingest while it runs.

See :ref:`add_index_command` for more details on the command line tools.

.. _accumulo_index_versions:

Accumulo Index Versions
-----------------------

See :ref:`index_versioning` for an explanation of index versions. The following versions are available in Accumulo:

.. tabs::

    .. tab:: Z3

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.1.0           Initial implementation
        2             1.2.1           Support for non-point geometries

                                      Support for shards
        3             1.2.5           Removed support for non-point geometries in favor of xz

                                      Removed redundant feature ID in row value to reduce size on disk

                                      Support for per-attribute visibility
        4             1.3.1           Support for table sharing
        5             2.0.0           Uses fixed Z-curve implementation
        ============= =============== =================================================================

    .. tab:: Z2

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.2.2           Initial implementation
        2             1.2.5           Removed support for non-point geometries in favor of xz

                                      Removed redundant feature ID in row value to reduce size on disk

                                      Support for per-attribute visibility
        3             1.3.1           Optimized deletes
        4             2.0.0           Uses fixed Z-curve implementation
        ============= =============== =================================================================

    .. tab:: XZ3

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.2.5           Initial implementation
        ============= =============== =================================================================

    .. tab:: XZ2

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.2.5           Initial implementation
        ============= =============== =================================================================

    .. tab:: Attribute

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.0.0           Initial implementation
        2             1.1.0           Added secondary date index
        3             1.2.5           Removed redundant feature ID in row value to reduce size on disk

                                      Support for per-attribute visibility
        4             1.3.1           Added secondary Z index
        5             1.3.2           Support for shards
        6             2.0.0-m.1       Internal row layout change
        7             2.0.0           Uses fixed Z-curve implementation
        ============= =============== =================================================================

    .. tab:: ID

        ============= =============== =================================================================
        Index Version GeoMesa Version Notes
        ============= =============== =================================================================
        1             1.0.0           Initial implementation
        2             1.2.5           Removed redundant feature ID in row value to reduce size on disk

                                      Support for per-attribute visibility
        3             2.0.0           Standardized index identifier to 'id'
        ============= =============== =================================================================

Note that GeoMesa versions prior to 1.2.2 included a geohash index. That index has been replaced with
the Z indices and is no longer supported.
