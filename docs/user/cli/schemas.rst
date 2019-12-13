Schema Commands
===============

These commands are used to create, describe and delete schemas (``SimpleFeatureType``\ s). Required
parameters are indicated with a ``*``.

.. _cli_create_schema:

``create-schema``
-----------------

Used to create a new ``SimpleFeatureType``.

======================== ==================================================
Argument                 Description
======================== ==================================================
``-c, --catalog *``      The catalog table used to hold the schema metadata
``-s, --spec *``         The ``SimpleFeatureType`` specification to create
``-f, --feature-name``   The name of the schema to create
``--dtg``                The attribute to use for the default date field
======================== ==================================================

The ``--spec`` argument may be any of the following:

* A string of attributes, for example ``name:String,dtg:Date,*geom:Point:srid=4326``
* The name of a ``SimpleFeatureType`` already available on the classpath
* A string of attributes, defined as a TypeSafe configuration
* The name of a file containing one of the above

See :ref:`cli_sft_conf` for more details on specifying the ``SimpleFeatureType``.

The ``--feature-name`` attribute is required if it is not implied by the specification string.
It may also be used to override the implied feature name.

``delete-catalog``
------------------

Deletes all ``SimpleFeatureType``\ s in a given catalog, and all features associated with them.

======================== ==============================================================
Argument                 Description
======================== ==============================================================
``-c, --catalog *``      The catalog table used to hold the schema metadata
======================== ==============================================================

``describe-schema``
-------------------

Describes the attributes of an existing ``SimpleFeatureType``.

======================== ==================================================
Argument                 Description
======================== ==================================================
``-c, --catalog *``      The catalog table containing the schema metadata
``-f, --feature-name *`` The name of the schema to describe
======================== ==================================================

``gen-avro-schema``
-------------------

Generate an Avro schema based on a ``SimpleFeatureType``.

======================== ==================================================
Argument                 Description
======================== ==================================================
``-s, --spec *``         The ``SimpleFeatureType`` specification to create
``-f, --feature-name``   The name of the schema to create
======================== ==================================================

See :ref:`cli_create_schema` for details on specifying a ``SimpleFeatureType``.

``get-sft-config``
------------------

Exports the ``SimpleFeatureType`` metadata.

======================== ====================================================
Argument                 Description
======================== ====================================================
``-c, --catalog *``      The catalog table used to hold the schema metadata
``-f, --feature-name *`` The name of the schema to export
``--format``             The format to output - either ``spec`` or ``config``
``--concise``            Export the metadata with minimal whitespace
``--exclude-user-data``  Exclude user data from the output
======================== ====================================================

The metadata can either be exported as a specification string, or as a TypeSafe
configuration file. See :ref:`cli_sft_conf` for more details on ``SimpleFeatureType``
formats.

``get-type-names``
------------------

Displays the names of ``SimpleFeatureType``\ s stored in a given catalog table.

=================== ============================================
Argument            Description
=================== ============================================
``-c, --catalog *`` The catalog table containing schema metadata
=================== ============================================

.. _manage_partitions_cli:

``manage-partitions``
---------------------

This command will list, add and delete partitioned tables used by GeoMesa. It has four sub-commands:

* ``list`` - list the partitions for a given schema
* ``add`` - create new partitions
* ``delete`` - delete existing partitions
* ``name`` - display the partition name associated with an attribute (i.e. date)

To invoke the command, use the command name followed by the sub-command, then any arguments. For example::

    $ geomesa manage-partitions list -c myCatalog ...

======================== =============================================================
Argument                 Description
======================== =============================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
======================== =============================================================

``list``
^^^^^^^^

The ``list`` sub-command will print out the current partitions for a schema.

``add/delete``
^^^^^^^^^^^^^^

The ``add`` and ``delete`` sub-commands will add or delete partitions, respectively. The ``add`` command
will create new tables as necessary, while the ``delete`` command will drop tables.

======================== =============================================================
Argument                 Description
======================== =============================================================
``--partition *``        The name of the partition to add or delete. May be specified
                         multiple times to operate on multiple partitions
``--force``              Force deletion of partitions without confirmation prompt
                         (delete only)
======================== =============================================================

To determine the appropriate partition name, use the ``name`` sub-command.

``name``
^^^^^^^^

The ``name`` sub-command will display the partition name associated with a particular date. The partition
names are required when adding or deleting partitions.

======================== =============================================================
Argument                 Description
======================== =============================================================
``--value *``            The date for the partition, in the form
                         ``yyyy-MM-ddTHH:mm:ss.SSSZ``. May be specified multiple
                         times to display multiple partition names
======================== =============================================================

``remove-schema``
-----------------

Deletes a ``SimpleFeatureType``, and all features associated with it.

======================== ==============================================================
Argument                 Description
======================== ==============================================================
``-c, --catalog *``      The catalog table used to hold the schema metadata
``-f, --feature-name``   The name of the schema to delete
``--pattern``            A regular expression matching the schemas to delete
``--force``              Delete any matching schemas without prompting for confirmation
======================== ==============================================================

The schema can either be specified by name, or a regular expression can be used to delete
multiple schemas at once.

.. _cli_update_schema:

``update-schema``
-----------------

Alter an existing ``SimpleFeatureType``. This command can be used to rename a schema, rename attributes,
append additional attributes, and modify keywords.

The schema metadata will be backed up before it is updated, generally in a newly created table. If there is an
error during the update, the backup can be used to restore the previous state.

.. warning::

  To ensure data integrity, all GeoMesa clients should be stopped before updating a schema, and restarted once
  the update is complete. In limited circumstances, clients can continue to run during the update, and they will
  pick up any modifications on the fly after a few minutes - however, it is safest to stop and restart them.

======================== ==============================================================
Argument                 Description
======================== ==============================================================
``-c, --catalog *``      The catalog table used to hold the schema metadata
``-f, --feature-name *`` The name of the schema to operate on
``--rename``             Change the name of the feature type
``--rename-attribute``   Change the name of an existing attribute
``--add-attribute``      Add a new attribute (column) to the feature type
``--enable-stats``       Enable or disable stats for the feature type
``--add-keyword``        Add a new keyword to the feature type user data
``--remove-keyword``     Delete an existing keyword from the feature type user data
``--rename-tables``      When renaming the feature type, update index tables to match
``--no-backup``          Disable backing up the schema before the update
======================== ==============================================================

The ``--rename`` parameter can be used to change the type name of the schema. The ``--rename-attribute``
parameter can be used to rename an attribute, by specifying the old name and the new name.

When renaming, the ``--rename-tables`` flag can be used to alter any index tables to match the new name(s),
but be aware that this can be a costly operation in some data stores.

The ``add-attribute`` parameter can be used to append additional columns to the end of the schema definition.
Columns should be defined in the standard GeoTools specification format, for example ``myColumn:String:index=true``.
See :ref:`attribute_types` for more information on column types. Any features that have already been written will
have a ``null`` value for the new columns. When adding columns that are attribute-indexed, the index will initially
be empty.

The ``enable-stats`` parameter can be used to permanently enable or disable cached statistics for the feature type.
See :ref:`stat_config` for more details on cached statistics.

The ``--add-keyword`` and ``--remove-keyword`` parameters can be used to add and/or remove keywords in the
user data of the schema. When adding a layer in GeoServer, the 'Keywords' section of the layer configuration page
will be automatically populated with the user data keywords.

Note that multiple attributes and/or keywords can be added/removed/renamed at once by specifying the parameters
multiple times.
