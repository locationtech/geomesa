.. _geomesa-process:

GeoMesa Processes
=================

The following analytic processes are available and optimized on GeoMesa
data stores, found in the ``geomesa-process`` module:

-  :ref:`arrow_conversion_process` - encodes simple features in the `Apache Arrow <https://arrow.apache.org/>`_ format
-  :ref:`bin_conversion_process` - encodes simple features in a minimized 16-byte format
-  :ref:`density_process` - computes a density heatmap for a CQL query
-  :ref:`date_offset_process` - modifies the specified date field in a feature collection by an input time period.
-  :ref:`hash_process`/:ref:`hash_color_process` - computes an additional 'hash' attribute which is useful for styling.
-  :ref:`join_process` - merges features from two different schemas using a common attribute field
-  :ref:`knn_process` - performs a KNN search
-  :ref:`point2point_process` - aggregates a collection of points into a collection of line segments
-  :ref:`proximity_process` - searches near a set input features
-  :ref:`query_process` - performs a Geomesa query, useful as input for nested requests
-  :ref:`routesearch_process` - matches features traveling along a given route
-  :ref:`sampling_process` - uses statistical sampling to reduces the features returned by a query
-  :ref:`statsiterator_process` - returns various stats for a CQL query
-  :ref:`tracklabel_process` - selects the last feature in a track based on a common attribute, useful for styling
-  :ref:`tubeselect_process` - performs a correlated search across time and space
-  :ref:`unique_process` - identifies unique values for an attribute

Where possible, the calculations are pushed out to a distributed system for faster performance. Currently
this has been implemented in the Accumulo data store and partially in the HBase data store. Other
back-ends can still be used, but local processing will be used.

Installation
------------

While they can be used independently, the common use case is to use them
with GeoServer. To deploy them in GeoServer requires:

1. a GeoMesa datastore plugin
2. the GeoServer WPS extension
3. the ``geomesa-process-wps_2.11-<version>.jar`` deployed in ``${GEOSERVER_HOME}/WEB-INF/lib``

.. note::

  Some processes also require custom output formats, available separately in the GPL licensed
  `GeoMesa GeoServer WFS module <https://github.com/geomesa/geomesa-geoserver>`__

The GeoMesa datastore plugin and GeoMesa process jars are both
available in the binary distribution in the gs-plugins directory.

Documentation about the GeoServer WPS Extension (including download
instructions) is available `here <http://docs.geoserver.org/stable/en/user/services/wps/install.html>`__.

To verify the install, start GeoServer, and you should see a line like
``INFO [geoserver.wps] - Found 15 bindable processes in GeoMesa Process Factory``.

In the GeoServer web UI, click 'Demos' and then 'WPS request builder'.
From the request builder, under 'Choose Process', click on any of the
'geomesa:' options to build up example requests and in some cases see
results.

Processors
----------

.. _arrow_conversion_process:

ArrowConversionProcess
^^^^^^^^^^^^^^^^^^^^^^

The ``ArrowConversionProcess`` converts an input feature collection to arrow format.

=====================  ===========
Parameters             Description
=====================  ===========
features               Input feature collection to encode.
includeFids            Include feature IDs in arrow file.
dictionaryFields       Attributes to dictionary encode.
useCachedDictionaries  Use cached top-k stats (if available), or run a dynamic stats query to build dictionaries.
sortField              Attribute to sort by.
sortReverse            Reverse the default sort order.
batchSize              Number of features to include in each record batch.
doublePass             Build dictionaries first, then query results in a separate scan.
=====================  ===========

.. _bin_conversion_process:

BinConversionProcess
^^^^^^^^^^^^^^^^^^^^^^

The ``BinConversionProcess`` converts an input feature collection to BIN format.

==========  ===========
Parameters  Description
==========  ===========
features    Input feature collection to query.
track       Track field to use for BIN records.
geom        Geometry field to use for BIN records.
dtg         Use cached top-k stats (if available), or run a dynamic stats query to build dictionaries.
label       Attribute to sort by.
axisOrder   Reverse the default sort order.
==========  ===========



.. _density_process:

DensityProcess
^^^^^^^^^^^^^^

The ``DensityProcess`` computes a density map over a set of features stored in GeoMesa. A raster image is returned.

============  ===========
Parameters    Description
============  ===========
data          Input Simple Feature Collection to run the density process over.
radiusPixels  Radius of the density kernel in pixels. Controls the "fuzziness" of the density map.
weightAttr    Name of the attribute to use for data point weights.
outputBBOX    Bounding box and CRS of the output raster.
outputWidth   Width of the output raster in pixels.
outputHeight  Height of the output raster in pixels.
============  ===========

.. _date_offset_process:

DateOffsetProcess
^^^^^^^^^^^^^^^^^

The ``DateOffsetProcess`` modifies the specified date field in a feature collection by an input time period.

============  ===========
Parameters    Description
============  ===========
data          Input features.
dateField     The date attribute to modify.
timeOffset    Time offset (e.g. P1D).
============  ===========



.. _hash_process:

HashAttributeProcess
^^^^^^^^^^^^^^^^^^^^

The ``HashAttributeProcess`` adds an attribute to each SimpleFeature that hashes the configured attribute modulo the configured param.

============  ===========
Parameters    Description
============  ===========
data          Input Simple Feature Collection to run the hash process over.
attribute     The attribute to hash on.
modulo        The divisor.
============  ===========

.. _hashExampleXML:

Hash example (XML)
"""""""""""""""""""

:download:`HashAttributeProcess_wps.xml </user/_static/process/HashProcess_wps.xml>` is a geoserver WPS call to the GeoMesa HashAttributeProcess. It can be run with the following curl call:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@HashAttributeProcess_wps.xml localhost:8080/geoserver/wps


The query should generate results that look like :download:`this </user/_static/process/HashProcess_results.json>`:

.. code-block:: json

    {
        "id" : "d0971735-f8fe-47ed-a7cd-2e12280e8ac1",
        "geometry" : {
            "coordinates" : [
                151.1554,
                18.2014
            ],
            "type" : "Point"
        },
        "type" : "Feature",
        "properties" : {
            "Vitesse" : 614,
            "Heading" : 244,
            "Date" : "2016-05-02T18:00:44.030+0000",
            "hash" : 237,
            "CabId" : 150002,
         }
     }


.. _hash_color_process:

HashAttributeColorProcess
^^^^^^^^^^^^^^^^^^^^^^^^^

The ``HashAttributeColorProcess`` adds an attribute to each SimpleFeature that hashes the configured attribute modulo the configured param and emit a color.

============  ===========
Parameters    Description
============  ===========
data          Input Simple Feature Collection to run the hash process over.
attribute     The attribute to hash on.
modulo        The divisor.
============  ===========

.. _join_process:

JoinProcess
^^^^^^^^^^^

The ``JoinProcess`` queries a feature type based on attributes from a second feature type.

=============  ===========
Parameters     Description
=============  ===========
primary        Primary feature collection being queried.
secondary      Secondary feature collection to be joined.
joinAttribute  Attribute field to join on.
joinFilter     Additional filter to apply to joined features.
attributes     Attributes to return. Attribute names should be qualified with the schema name, e.g. foo.bar.
=============  ===========



.. _knn_process:

KNearestNeighborProcess
^^^^^^^^^^^^^^^^^^^^^^^

The ``KNearestNeighborProcess`` performs a K Nearest Neighbor search on a Geomesa feature collection using another feature collection as input. Return k neighbors for each point in the input data set. If a point is the nearest neighbor of multiple points of the input data set, it is returned only once.

=================  ===========
Parameters         Description
=================  ===========
inputFeatures      Input feature collection that defines the KNN search.
dataFeatures       The data set to query for matching features.
numDesired         K : number of nearest neighbors to return.
estimatedDistance  Estimate of Search Distance in meters for K neighbors---used to set the granularity of the search.
maxSearchDistance  Maximum search distance in meters---used to prevent runaway queries of the entire table.
=================  ===========

.. _knnExampleXML:

K-Nearest-Neighbor example (XML)
""""""""""""""""""""""""""""""""

:download:`KNNProcess_wps.xml </user/_static/process/KNNProcess_wps.xml>` is a geoserver WPS call to the GeoMesa KNearestNeighborProcess. It is here chained with a Query process (see :ref:`chaining_processes`) in order to avoid points related to the same Id to be matched by the request. It can be run with the following curl call:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@KNNProcess_wps.xml localhost:8080/geoserver/wps

.. _point2point_process:

Point2PointProcess
^^^^^^^^^^^^^^^^^^

The ``Point2PointProcess`` aggregates a collection of points into a collection of line segments.

=====================  ===========
Parameters             Description
=====================  ===========
data                   Input feature collection.
groupingField          Field on which to group.
sortField              Field on which to sort (must be Date type).
minimumNumberOfPoints  Minimum number of points.
breakOnDay             Break connections on day marks.
filterSingularPoints   Filter out segments that fall on the same point.
=====================  ===========

.. _point2pointExampleXML:

Point2Point example (XML)
"""""""""""""""""""""""""

:download:`Point2PointProcess_wps.xml </user/_static/process/Point2PointProcess_wps.xml>` is a geoserver WPS call to the GeoMesa Point2PointProcess. It can be run with the following curl call:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@Point2PointProcess_wps.xml localhost:8080/geoserver/wps

.. _point2pointExampleResults:

The query should generate results that look like :download:`this </user/_static/process/Point2PointProcess_results.json>`:

.. code-block:: json

    {
        "id" : "367152240-4",
        "geometry" : {
            "coordinates" : [
                [
                    -13.4041,
                    37.8067
                ],
                [
                    -13.4041,
                    37.8068
                ]
            ],
            "type" : "LineString"
        },
        "type" : "Feature",
        "properties" : {
            "Date_end" : "2018-02-05T14:54:36.598+0000",
            "CabId" : 367152240,
            "Date_start" : "2018-02-05T14:53:58.078+0000"
        }
    }



.. _proximity_process:

ProximitySearchProcess
^^^^^^^^^^^^^^^^^^^^^^

The ``ProximitySearchProcess`` performs a proximity search on a Geomesa feature collection using another feature collection as input.

=====================  ===========
Parameters             Description
=====================  ===========
inputFeatures          Input feature collection that defines the proximity search.
dataFeatures           The data set to query for matching features.
bufferDistance         Buffer size in meters.
=====================  ===========

.. _proximityExampleXML:

Proximity search example (XML)
""""""""""""""""""""""""""""""

:download:`ProximitySearchProcess_wps.xml </user/_static/process/ProximitySearchProcess_wps.xml>` is a geoserver WPS call to the GeoMesa ProximitySearchProcess. It can be run with the following curl call:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@ProximitySearchProcess_wps.xml localhost:8080/geoserver/wps


.. _routesearch_process:

RouteSearchProcess
^^^^^^^^^^^^^^^^^^

The ``RouteSearchProcess`` finds features around a route that are heading along the route and not just crossing over it.

================  ===========
Parameters        Description
================  ===========
features          Input feature collection to query.
routes            Routes to search along. Features must have a geometry of LineString.
bufferSize        Buffer size (in meters) to search around the route.
headingThreshold  Threshold for comparing headings, in degrees.
routeGeomField    Attribute that will be examined for routes to match. Must be a LineString.
geomField         Attribute that will be examined for route matching.
bidirectional     Consider the direction of the route or just the path of the route.
headingField      Attribute that will be examined for heading in the input features. If not provided, input features geometries must be LineStrings.
================  ===========

.. _routeSearchExampleXML:

Route search example (XML)
""""""""""""""""""""""""""

:download:`RouteSearchProcess_wps.xml </user/_static/process/RouteSearchProcess_wps.xml>` is a geoserver WPS call to the GeoMesa RouteSearchProcess. It can be run with the following curl call:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@RouteSearchProcess_wps.xml localhost:8080/geoserver/wps



.. _sampling_process:

SamplingProcess
^^^^^^^^^^^^^^^

The ``SamplingProcess`` uses statistical sampling to reduces the features returned by a query.

=============  ===========
Parameters     Description
=============  ===========
data           Input features.
samplePercent  Percent of features to return, between 0 and 1.
threadBy       Attribute field to link associated features for sampling.
=============  ===========

.. _samplingExampleXML:

Sampling example (XML)
""""""""""""""""""""""

:download:`SamplingProcess_wps.xml </user/_static/process/SamplingProcess_wps.xml>` is a geoserver WPS call to the GeoMesa SamplingProcess. It can be run with the following curl call:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@SamplingProcess_wps.xml localhost:8080/geoserver/wps



.. _statsiterator_process:

StatsProcess
^^^^^^^^^^^^

The ``StatsProcess`` allows the running of statistics on a given feature set.

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

+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| Syntax                                                            |   Parameters         | Description                                             |
+===================================================================+======================+=========================================================+
| Count                                                                                                                                              |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| ``Count()``                                                       |                      | Counts the number of features.                          |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| MinMax                                                                                                                                             |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| ``MinMax(attribute)``                                             | * attribute*: String | Finds the min and max values of the given attribute.    |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| GroupBy                                                                                                                                            |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| ``GroupBy(attribute,stat)``                                       | * attribute*: String | Groups stats by the given attribute and then runs       |
|                                                                   | * stat: Stat String  | the given stat on each group. Any stat can be provided. |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| Descriptive Stats                                                                                                                                  |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| ``DescriptiveStats(attribute)``                                   | * attribute*: String | Runs single pass stats on the given attribute           |
|                                                                   |                      | calculating stats describing the attribute such as:     |
|                                                                   |                      | count; min; max; mean; and population and sample        |
|                                                                   |                      | versions of variance, standard deviation, kurtosis,     |
|                                                                   |                      | excess kurtosis, covariance, and correlation.           |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| Enumeration                                                                                                                                        |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| ``Enumeration(attribute)``                                        | * attribute*: String | Enumerates the values in the give attribute and the     |
|                                                                   |                      | number of occurrences.                                  |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| TopK                                                                                                                                               |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| ``TopK(attribute)``                                               | * attribute*: String | TopK of the given attribute                             |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| Histogram                                                                                                                                          |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| ``Histogram(attribute,numBins,lower,upper)``                      | * attribute*: String | Provides a histogram of the given attribute, binning    |
|                                                                   | * numBins: Int       | the results into a binned array using the numBins as    |
|                                                                   | * lower: Int         | the number of bins and lower and upper as the bounds    |
|                                                                   | * upper: Int         | of the binned array.                                    |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| Frequency                                                                                                                                          |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| ``Frequency(attribute,dtg,period,precision)``                     | * attribute*: String | Estimates frequency counts at scale.                    |
|                                                                   | * dtg*: String       |                                                         |
|                                                                   | * period: TimePeriod |                                                         |
|                                                                   | * precision: Int     |                                                         |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| z3Histogram                                                                                                                                        |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| ``Z3Histogram(geom,dtg,period,length)``                           | * geom*: String      | Provides a histogram similar to ``Histogram`` but       |
|                                                                   | * dtg*: String       | treats the geometry and date attributes as a single     |
|                                                                   | * period: TimePeriod | value.                                                  |
|                                                                   | * length: Int        |                                                         |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| z3Frequency                                                                                                                                        |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| ``Z3Frequency(geom,dtg,period,precision)``                        | * geom*: String      | Provides a freqency estimate similar to ``Frequency``   |
|                                                                   | * dtg*: String       | but treats the geometry and date attributes as a        |
|                                                                   | * period: TimePeriod | single value.                                           |
|                                                                   | * precision: Int     |                                                         |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| Iterator Stack                                                                                                                                     |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| ``IteratorStackCount()``                                          |                      | IteratorStackCount keeps track of the number of times   |
|                                                                   |                      | Accumulo sets up an iterator stack as a result of a     |
|                                                                   |                      | query.                                                  |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+

.. _tracklabel_process:

TrackLabelProcess
^^^^^^^^^^^^^^^^^

The ``TrackLabelProcess`` returns a single feature that is the head of a track of related simple features.

==========  ===========
Parameters  Description
==========  ===========
data        Input features.
track       Track attribute to use for grouping features.
dtg         Date attribute to use for ordering tracks.
==========  ===========

.. _trackLabelExampleXML:

TrackLabel example (XML)
""""""""""""""""""""""""

:download:`TrackLabelProcess_wps.xml </user/_static/process/TrackLabelProcess_wps.xml>` is a geoserver WPS call to the GeoMesa TrackLabelProcess. It can be run with the following curl call:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@TrackLabelProcess_wps.xml localhost:8080/geoserver/wps




.. _tubeselect_process:

TubeSelectProcess
^^^^^^^^^^^^^^^^^

The ``TubeSelectProcess`` performs a tube select on a Geomesa feature collection based on another feature collection. To get more informations on ``TubeSelectProcess`` and how to use it, you can read `this tutorial <http://www.geomesa.org/documentation/tutorials/geomesa-tubeselect.html>`__. 

=================   ===========
Parameters          Description
=================   ===========
tubeFeatures        Input feature collection (must have geometry and datetime).
featureCollection   The data set to query for matching features.
filter              The filter to apply to the featureCollection.
maxSpeed            Max speed of the object in m/s for nofill & line gapfill methods.
maxTime             Time as seconds for nofill & line gapfill methods.
bufferSize          Buffer size in meters to use instead of maxSpeed/maxTime calculation.
maxBins             Number of bins to use for breaking up query into individual queries.
gapFill             Method of filling gap (nofill, line).
=================   ===========

.. _tubeSelectExampleXML:

TubeSelect example (XML)
""""""""""""""""""""""""

:download:`TubeSelectProcess_wps.xml </user/_static/process/TubeSelectProcess_wps.xml>` is a geoserver WPS call to the GeoMesa TubeSelectProcess. It can be run with the following curl call:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@TubeSelectProcess_wps.xml localhost:8080/geoserver/wps




.. _query_process:

QueryProcess
^^^^^^^^^^^^

The ``QueryProcess`` takes an (E)CQL query/filter for a given feature set as a text object and returns
the result as a json object.

==========  ===========
Parameters  Description
==========  ===========
features    The data source feature collection to query. Reference as ``store:layername``.
		        For an XML file enter ``<wfs:Query typeName=store:layername />``
		        For interactive WPS request builder select ``VECTOR_LAYER`` & choose ``store:layername``

filter      The filter to apply to the feature collection.
		        For an XML file enter:

            .. code-block:: xml

	    	<wps:ComplexData mimeType="text/plain; subtype=cql">
		   <![CDATA[some-query-text]]
	 	</wps:ComplexData>


	    For interactive WPS request builder select TEXT & choose ``"text/plain; subtype=cql"``
		enter the query text in the text box

output      Specify how the output feature collection will be presented.
		        For an XML file enter:

            .. code-block:: xml

                <wps:ResponseForm>
                   <wps:RawDataOutput mimeType="application/json">
                      <ows:Identifier>result</ows:Identifier>
                   </wps:RawDataOutput>
                </wps:ResponseForm>


       	    For interactive WPS request builder check the Generate box and choose "application/json"

properties  The properties / transforms to apply before gathering stats.
==========  ===========

.. _queryExampleXML:

Query example (XML)
"""""""""""""""""""

:download:`QueryProcess_wps.xml </user/_static/process/QueryProcess_wps.xml>` is a geoserver WPS call to the GeoMesa QueryProcess that performs the same query shown
in the `Accumulo-quickstart <http://www.geomesa.org/documentation/tutorials/geomesa-quickstart-accumulo.html>`_. It can be run with the following curl call:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@QueryProcess_wps.xml localhost:8080/geoserver/wps


The query should generate results that look like :download:`this </user/_static/process/QueryProcess_results.json>`:

.. code-block:: json

    {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "geometry": {
            "type": "Point",
            "coordinates": [
              -76.513,
              -37.4941
            ]
          },
          "properties": {
            "Who": "Bierce",
            "What": 931,
            "When": "2014-07-04T22:25:38.000+0000"
          },
          "id": "Observation.931"
        }
      ]
    }

.. _unique_process:

UniqueProcess
^^^^^^^^^^^^^

The ``UniqueProcess`` class is optimized for GeoMesa to find unique attributes values for a feature collection,
which are returned as a json object.

===========  ===========
Parameters   Description
===========  ===========
features     The data source feature collection to query. Reference as ``store:layername``.
		        For an XML file enter ``<wfs:Query typeName=store:layername />``
		        For interactive WPS request builder select ``VECTOR_LAYER`` & choose ``store:layername``

attribute    The attribute for which unique values will be extracted. Attributes are expressed as a string.
		        For an XML file enter ``<wps:LiteralData>attribute-name</wps:LiteralData>``

filter       The filter to apply to the feature collection.
		        For an XML file enter:

             .. code-block:: xml

    		<wps:ComplexData mimeType="text/plain; subtype=cql">
     		   <![CDATA[some-query-text]]
    		</wps:ComplexData>


	     For interactive WPS request builder select TEXT & choose ``"text/plain; subtype=cql"``
		 enter the query text in the text box.

histogram    Create a histogram of attribute values. Expressed as a boolean (true/false).
		        For an XML file enter ``<wps:LiteralData>true/false</wps:LiteralData>``

sort         Sort the results. Expressed as a string; allowed values are ASC or DESC.
		        For an XML file enter ``<wps:LiteralData>ASC/DESC</wps:LiteralData>``

sortByCount  Sort by histogram counts instead of attribute values. Expressed as a boolean (true/false).
		        For an XML file enter ``<wps:LiteralData>true/false</wps:LiteralData>``

output       Specify how the output feature collection will be presented.
		        For an XML file enter:

             .. code-block:: xml

                <wps:ResponseForm>
                   <wps:RawDataOutput mimeType="application/json">
                      <ows:Identifier>result</ows:Identifier>
                   </wps:RawDataOutput>
                </wps:ResponseForm>


	     For interactive WPS request builder check the Generate box and choose "application/json"
===========  ===========

.. _uniqueExampleXML:

Unique example (XML)
""""""""""""""""""""

:download:`UniqueProcess_wps.xml </user/_static/process/UniqueProcess_wps.xml>` is a geoserver WPS call to the GeoMesa UniqueProcess that reports the unique names
in in the 'Who' field of the Accumulo quickstart data for a restricted bounding box (-77.5, -37.5, -76.5, -36.5)). It can be run with the following curl call:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@UniqueProcess_wps.xml localhost:8080/geoserver/wps

.. _uniqueExampleResults:

The query should generate results that look like this:

.. code-block:: json

	{
	  "type": "FeatureCollection",
	  "features": [
	    {
	      "type": "Feature",
	      "properties": {
		"value": "Addams",
		"count": 37
	      },
	      "id": "fid--21d4eb0_15b68e0e8ca_-7fd6"
	    },
	    {
	      "type": "Feature",
	      "properties": {
		"value": "Bierce",
		"count": 43
	      },
	      "id": "fid--21d4eb0_15b68e0e8ca_-7fd5"
	    },
	    {
	      "type": "Feature",
	      "properties": {
		"value": "Clemens",
		"count": 48
	      },
	      "id": "fid--21d4eb0_15b68e0e8ca_-7fd4"
	    }
	  ]
	}

.. _chaining_processes:

Chaining Processes
^^^^^^^^^^^^^^^^^^

WPS processes can be chained, using the result of one process as the input for another. For example, a bounding box
in a GeoMesa :ref:`query_process` can be used to restrict data sent to :ref:`statsiterator_process`. 
:download:`GeoMesa_WPS_chain_example.xml </user/_static/process/GeoMesa_WPS_chain_example.xml>` will get all points from
the AccumuloQuickStart table that are within a specified bounding box (-77.5, -37.5, -76.5, -36.5), and calculate
descriptive statistics on the 'What' attribute of the results.


The query should generate results that look like this:

.. code-block:: json

	{
	  "type": "FeatureCollection",
	  "features": [
	    {
	      "type": "Feature",
	      "geometry": {
		"type": "Point",
		"coordinates": [
		  0,
		  0
		]
	      },
	      "properties": {
		"stats": "{\"count\":128,\"minimum\":[29.0],\"maximum\":[991.0],\"mean\":[508.5781249999999],\"population_variance\":[85116.25952148438],\"population_standard_deviation\":[291.74691004616375],\"population_skewness\":[-0.11170819256679464],\"population_kurtosis\":[1.7823482287566166],\"population_excess_kurtosis\":[-1.2176517712433834],\"sample_variance\":[85786.46628937007],\"sample_standard_deviation\":[292.893267743337],\"sample_skewness\":[-0.11303718280959842],\"sample_kurtosis\":[1.8519712064424219],\"sample_excess_kurtosis\":[-1.1480287935575781],\"population_covariance\":[85116.25952148438],\"population_correlation\":[1.0],\"sample_covariance\":[85786.46628937007],\"sample_correlation\":[1.0]}"
	      },
	      "id": "stat"
	    }
	  ]
	}
