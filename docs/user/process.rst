GeoMesa Processes
=================

The following analytic processes are available and optimized on GeoMesa
data stores, found in the ``geomesa-process`` module:

-  :ref:`density_process` - computes a density heatmap for a CQL query
-  ``hash_attribute_process`` - computes an
   additional 'hash' attribute which is useful for styling.
-  ``knn_search_process`` - performs a KNN search
-  ``point2point_process`` - aggregates a collection of points into a
   collection of line segments
-  ``proximity_search_process`` - performs a nearest neighbor search
-  ``sampling_process`` - uses statistical sampling to reduces the features
   returned by a query
-  :ref:`StatsIteratorProcess` - returns various stats for a CQL query
-  ``TubeSelectProcess`` - performs a correlated search across
   time/space dimensions
-  :ref:`QueryProcess` - optimizes GeoMesa queries in WPS chains
-  ``UniqueProcess`` - identifies unique values for an attribute in
   results of a CQL query
-  ``JoinProcess`` - returns merged features from two different schemas
   using a common attribute field

Installation
------------

The above extensions are particular to the Accumulo data store.

While they can be used independently, the common use case is to use them
with GeoServer. To deploy them in GeoServer, one will require a) the
GeoMesa Accumulo datastore plugin, b) the GeoServer WPS extension, and
c) the ``geomesa-process-${VERSION}.jar`` to be deployed in
``${GEOSERVER_HOME}/WEB-INF/lib``.

The GeoMesa Accumulo datastore plugin and GeoMesa process jars are both
available in the binary distribution in the gs-plugins directory.

Documentation about the GeoServer WPS Extension (including download
instructions) is available here:
http://docs.geoserver.org/stable/en/user/services/wps/install.html.

To verify the install, start GeoServer, and you should see a line like
``INFO [geoserver.wps] - Found 11 bindable processes in GeoMesa Process Factory``.

In the GeoServer web UI, click 'Demos' and then 'WPS request builder'.
From the request builder, under 'Choose Process', click on any of the
'geomesa:' options to build up example requests and in some cases see
results.

Processors
----------

.. _density_process:

DensityProcess
^^^^^^^^^^^^^^



.. _statsiterator_process:

StatsIteratorProcess
^^^^^^^^^^^^^^^^^^^^

The ``StatsIteratorProcess`` allows the running of statistics on a given feature set. The statics calculations are pushed
down to the Accumulo iterators allowing for very fast performance.

==========  ===========
Parameters  Description
==========  ===========
features    The feature set on which to query. Can be a raw text input, reference to a remote URL, a subquery or a vector layer.
statString  Stat string indicating which stats to instantiate. More info here :ref:`statString`.
encode      Return the values encoded as json. Must be ``true`` or ``false``; empty values will not work.
properties  The properties / transforms to apply before gathering stats.
==========  ===========

.. _statString:

Stat Strings
""""""""""""

Stat strings are a GeoMesa domain specific language (DSL) that allows the specification of stats for the iterators
to collect. The available stat function are listed below:

.. note::

    Items marked with ``*`` are the name of an attribute, either in your sft or as the result of a transformation or projection.

.. note::

    A TimePeriod is defined as one of the following strings: "day", "week", "month", "year"

=================  ===============================================  ==================  ===========
Stat               Syntax                                           Parameters          Description
=================  ===============================================  ==================  ===========
Count              ``Count()``                                                          Counts the number of features.
MinMax             ``MinMax(attribute)``                            -*attribute: String  Finds the Min and Max values of the given attribute.
GroupBy            ``GroupBy(attribute,stat)``                      -*attribute: String  Groups Stats by the given attribute and then runs
                                                                    -stat: Stat String   the given stat on each group. Any stat can be provided.
Descriptive Stats  ``DescriptiveStats(attribute)``                  -*attribute: String  Runs single pass stats on the given attribute
                                                                                        calculating stats describing the attribute such as:
                                                                                        Count; Min; Max; Mean; and Population and Sample
                                                                                        versions of Variance, Standard Deviation, Kurtosis,
                                                                                        Excess Kurtosis, Covariance, and Correlation.
Enumeration        ``Enumeration(attribute)``                       -*attribute: String  Enumerates the values in the give attribute and the
                                                                                        number of occurrences.
TopK               ``TopK(attribute)``                              -*attribute: String  TopK of the given attribute
Histogram          ``Histogram(attribute,numBins,lower,upper)``     -*attribute: String  Provides a histogram of the given attribute, binning
                                                                    -numBins: Int        the results into a Binned Array using the numBins as
                                                                    -lower: Int          the number of Bins and lower and upper as the bounds
                                                                    -upper: Int          of the Binned Array.
Freqency           ``Frequency(attribute,dtg,period,precision)``    -*attribute: String  Estimates frequency counts at scale.
                                                                    -*dtg: String
                                                                    -period: TimePeriod
                                                                    -precision: Int
z3Histogram        ``Z3Histogram(geom,dtg,period,length)``          -*geom: String       Provides a histogram similar to ``Histogram`` but
                                                                    -*dtg: String        treats the geometry and date attributes as a single
                                                                    -period: TimePeriod  value.
                                                                    -length: Int
z3Frequency        ``Z3Frequency(geom,dtg,period,precision)``       -*geom: String       Provides a freqency estimate similar to ``Frequency``
                                                                    -*dtg: String        but treats the geometry and date attributes as a
                                                                    -period: TimePeriod  single value.
                                                                    -precision: Int
Iterator Stack     ``IteratorStackCount()``                                             IteratorStackCount keeps track of the number of times
                                                                                        Accumulo sets up an iterator stack as a result of a
                                                                                        query.
=================  ===============================================  ==================  ===========

.. _query_process:

QueryProcess
^^^^^^^^^^^^
