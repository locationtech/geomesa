.. _ageoff_accumulo:

Age-Off Iterators
=================

This chapter provides documentation on how to configure the beta feature for data age-off via Accumulo iterators.
Age-off allows administrators to set retention periods on data (e.g. 3 months) in order to automatically hide and
delete data from tables without manually deleting features.

Currently there are two types of age-off supported - attribute based, and ingest-time based.

Installation
------------

The age-off iterators are provided as part of the GeoMesa Accumulo Distributed Runtime jar which can be found in the
:doc:`install` chapter.

Configuration
-------------

Age-off can be configured through the GeoMesa command-line tools, using the ``configure-age-off`` command.
See :ref:`accumulo_tools` for an overview of the command-line tools.

.. warning::

    If age-off iterators have previously been configured manually, it is suggested to remove them before
    using any provided tools, in order to ensure consistency in naming and priority.

Viewing Current Age-Off
^^^^^^^^^^^^^^^^^^^^^^^

Any configured age-off iterators can be shown via the command line tools ``configure-age-off`` command::

    $ geomesa-accumulo configure-age-off -c test_catalog -f test_feature --list
    INFO  Attribute age-off: None
    INFO  Timestamp age-off: name:age-off, priority:10, class:org.locationtech.geomesa.accumulo.iterators.AgeOffIterator, properties:{retention=PT1M}

Note that this may not display any manually configured age-off iterators, as the iterator name may not match.

Ingest Time Age-Off
^^^^^^^^^^^^^^^^^^^

Ingest time age-off will use the timestamp associated with the Accumulo data value to validate retention.

.. note::

    Ingest time age-off requires that the Accumulo tables are configured with system timestamps, instead of
    the default logical timestamps. See :ref:`logical_timestamps` for more information.

Ingest time age-off can be configured via the command line tools ``configure-age-off`` command, without specifying
a date attribute::

    $ geomesa-accumulo configure-age-off -c test_catalog -f test_feature --set --expiry '1 day'

This will remove any existing age-off configuration and replace it with the new specification.

Attribute Age-Off
^^^^^^^^^^^^^^^^^

Attribute age-off will use a date-type attribute to validate retention.

Attribute age-off can be configured via the command line tools ``configure-age-off`` command by specifying
a date attribute::

    $ geomesa-accumulo configure-age-off -c test_catalog -f test_feature --set --expiry '1 day' --dtg my_date_attribute

This will remove any existing age-off configuration and replace it with the new specification.

Removing Age-Off
^^^^^^^^^^^^^^^^

Any configured age-off iterators can be cleared via the command line tools ``configure-age-off`` command::

    $ geomesa-accumulo configure-age-off -c test_catalog -f test_feature --remove

This will remove both attribute and ingest time age-off.

Statistics
----------

As features are aged off, summary data statistics will get out of date, which can degrade query planning. For
manageable data sets, it is recommended to re-analyze statistics every so often, via the
:ref:`accumulo_tools_stats_analyze` command. If the data set is too large for this to be feasible, then stats
can instead be disabled completely via :ref:`stats_generate_config`.

Forcing Deletion of Records
---------------------------

The GeoMesa age-off iterators will not fully delete records until compactions occur. To force a true deletion of data
on disk, you must manually compact a table or range. When compacting an entire table you should take care not to
overwhelm your system. To facilitate this, you may use the GeoMesa Accumulo command-line :ref:`compact_command`
command.
