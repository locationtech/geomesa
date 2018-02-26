Query and Export Commands
=========================

These commands are used to query and export simple features. Required parameters are indicated with a ``*``.

``convert``
-----------

The convert command is used to directly transform data in one format into another, without ingesting them
into GeoMesa. For example, it could be used to convert CSV files to GeoJSON.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-C, --converter *``    The GeoMesa converter used to create ``SimpleFeature``\ s
``-s, --spec *``         The ``SimpleFeatureType`` specification to create
``-f, --feature-name``   The name of the schema
``-q, --cql``            CQL filter to select features to export
``-m, --max-features``   Limit the number of features exported
``-F, --output-format``  Output format used for export
``-o, --output``         Output to a file instead of standard out
``--hints``              Query hints used to modify the query
``--index``              Specific index used to run the query
``--no-header``          Don't export the type header, for CSV and TSV formats
``--gzip``               Level of gzip compression to use for output, from 1-9
======================== =========================================================

See :ref:`cli_export` and :ref:`cli_ingest` for a description of the arguments.

.. _cli_explain:

``explain``
-----------

The explain command can be used to debug queries that are slow or problematic. Without actually running the query,
it will show a variety of data, including the index being used, any query hints extracted, the exact ranges being
scanned and filters being applied.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
``-q, --cql *``          CQL filter to select features to export
``-a, --attributes``     Specific attributes to export
``--hints``              Query hints used to modify the query
``--index``              Specific index used to run the query
======================== =========================================================

See :ref:`cli_export` for a description of the arguments.

.. _cli_export:

``export``
----------

Export features in a variety of formats.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
``-q, --cql``            CQL filter to select features to export
``-a, --attributes``     Specific attributes to export
``-m, --max-features``   Limit the number of features exported
``-F, --output-format``  Output format used for export
``-o, --output``         Output to a file instead of standard out
``--hints``              Query hints used to modify the query
``--index``              Specific index used to run the query
``--no-header``          Don't export the type header, for CSV and TSV formats
``--gzip``               Level of gzip compression to use for output, from 1-9
======================== =========================================================


The ``--attributes`` argument can be used to select a subset of attributes to export, or to transform
the attributes using filter functions. A simple projection can be specified as a comma-delimited list::

    --attributes name,age,geom

The feature ID can be specified along with other attribute names, with the reserved word ``id``::

    --attributes id,name,age,geom

Transforms can be accomplished by specifying transform functions::

    --attributes id,name,name_transform=strConcat(name, 'foo')

For a full list of transforms, see the GeoTools `documentation <http://docs.geotools.org/latest/userguide/library/main/function_list.html>`_.
Note that not all functions work in transforms, however.

The ``--output-format`` argument defines the encoding used for export. Currently, it can be one of:

* ``arrow`` Apache Arrow streaming file format
* ``avro`` Apache Avro format
* ``bin`` Custom minimal binary encoding
* ``csv``, ``tsv``
* ``geojson``, ``json``
* ``gml`` `Geography Markup Language <http://www.opengeospatial.org/standards/gml>`_
* ``shp`` ESRI Shapefile
* ``null`` suppress output entirely

The ``--output`` argument can be used to export to a file. By default, export data is written to the standard
output stream.

The ``--hints`` argument can be used to set query hints. Hints should be specified as ``key1=value1;key2=value2``, etc.
Note that due to shell expansion, the hint string will likely need to be quoted. See :ref:`analytic_queries` for
examples of query hints that can be set. Note that query hints may cause errors if they don't correspond to the
output format specified.

The ``--index`` argument can be used to force the query to run against a particular index, instead of using
the best index as determined through query planning. The argument should be the name of an index, e.g. ``id``
or ``z3``. See :ref:`index_overview` for a list of valid indices. Note that not all schemas will
have all index types.

The ``--gzip`` argument can be used to compress the output through **gzip** encoding. It can be specified
as a number between 1-9. Higher numbers indicate more compression, lower numbers indicate faster compression.

``export-leaflet``
------------------

Export features to a leaflet map.

.. note::

    This command is intended for testing and small scale data exploration and visualization only. For production map generation it is highly recommended to use GeoServer.

======================== =========================================================
Argument                 Description
======================== =========================================================
``-c, --catalog *``      The catalog table containing schema metadata
``-f, --feature-name *`` The name of the schema
``-q, --cql``            CQL filter to select features to export
``-a, --attributes``     Specific attributes to export
``-m, --max-features``   Limit the number of features exported
``-F, --output-format``  Output format used for export
``-o, --output``         Output to a file instead of standard out
``--hints``              Query hints used to modify the query
``--index``              Specific index used to run the query
======================== =========================================================

This command is similar to the :ref:`cli_export` command, however, this command draws the features on a leaflet map and, if possible, opens the map in your browser. This allows you to view data stored in GeoMesa without requiring the use of GeoServer or other 3rd party mapping tools.

The generated map provides both a feature and heat map. The feature map provides the ability to click a point in order to view the attributes of that feature.

.. warning::

    This command requires the use of an online browser in order to access online resources.


The ``--attributes`` argument can be used to select a subset of attributes to export, or to transform
the attributes using filter functions. A simple projection can be specified as a comma-delimited list::

    --attributes name,age,geom

The feature ID can be specified along with other attribute names, with the reserved word ``id``::

    --attributes id,name,age,geom

Transforms can be accomplished by specifying transform functions::

    --attributes id,name,name_transform=strConcat(name, 'foo')

For a full list of transforms, see the GeoTools `documentation <http://docs.geotools.org/latest/userguide/library/main/function_list.html>`_.
Note that not all functions work in transforms, however.

The ``--output`` argument can be used to set the location the Leaflet HTML is written to. By default, the data is written to the ``leaflet`` subdirectory in the current GeoMesa home, or if GeoMesa home is not set to ``/tmp/leaflet``. This parameter is useful for saving maps for later use.

The ``--hints`` argument can be used to set query hints. Hints should be specified as ``key1=value1;key2=value2``, etc.
Note that due to shell expansion, the hint string will likely need to be quoted. See :ref:`analytic_queries` for
examples of query hints that can be set. Note that query hints may cause errors if they don't correspond to the
output format specified.

The ``--index`` argument can be used to force the query to run against a particular index, instead of using
the best index as determined through query planning. The argument should be the name of an index, e.g. ``id``
or ``z3``. See :ref:`index_overview` for a list of valid indices. Note that not all schemas will
have all index types.
