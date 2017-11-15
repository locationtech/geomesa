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

``keywords``
------------

View, add, or remove keywords associated with a ``SimpleFeatureType``. Keywords are stored as user data,
and will automatically populate GeoServer layers when the layer is created.

======================== ==============================================================
Argument                 Description
======================== ==============================================================
``-c, --catalog *``      The catalog table used to hold the schema metadata
``-f, --feature-name *`` The name of the schema to operate on
``-l, --list``           List existing keywords
``-a, --add``            Add a new keyword by name
``-r, --remove``         Delete an existing keyword by name
``--removeAll``          Delete all existing keywords
======================== ==============================================================

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
