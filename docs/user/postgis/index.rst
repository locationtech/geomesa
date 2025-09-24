.. _postgis_index_page:

Partitioned PostGIS Data Store
==============================

GeoMesa provides an extension to the standard GeoTools PostGIS data store for better handling
of time series data. The GeoMesa extension leverages PostgreSQL's native partitioning to support
efficient spatio-temporal queries against large data sets.

.. info::

    GeoMesa currently supports PostgreSQL {{postgres_supported_versions}}.

.. warning::

    When using PostgreSQL 17, PostGIS must be version 3.5.2 or later.

.. toctree::
    :maxdepth: 1

    design
    install
    usage
    geoserver
    commandline
    index_config
    multi_tenancy
