.. _stats_process:

StatsProcess
^^^^^^^^^^^^

The ``StatsProcess`` allows the running of statistics on a given feature set. It accepts the following parameters:

==========  ===========
Parameter   Description
==========  ===========
features    The feature set on which to query
statString  Stat string indicating which stats to instantiate - see below
encode      Return the values encoded as json. Must be ``true`` or ``false``; empty values will not work
properties  The properties/transforms to apply before gathering stats
==========  ===========

Stat Strings
------------

Stat strings are a GeoMesa domain specific language (DSL) that allows the specification of stats for the iterators
to collect. See :ref:`statistical_queries` for an explanation of the available stats.
