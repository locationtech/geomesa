GeoMesa Processes
=================

The following analytic processes are available and optimized on GeoMesa
data stores, found in the ``geomesa-process`` module:

-  :ref:`density_process` - computes a density heatmap for a CQL query
-  ``hash_attribute_process`` - computes an
   additional 'hash' attribute which is useful for styling.
-  ``JoinProcess`` - returns merged features from two different schemas
   using a common attribute field
-  ``knn_search_process`` - performs a KNN search
-  ``point2point_process`` - aggregates a collection of points into a
   collection of line segments
-  ``proximity_search_process`` - performs a nearest neighbor search
-  :ref:`query_process` - performs a Geomesa optimized query using spatiotemporal indexes
-  ``sampling_process`` - uses statistical sampling to reduces the features
   returned by a query
-  :ref:`StatsIteratorProcess` - returns various stats for a CQL query
-  ``TubeSelectProcess`` - performs a correlated search across
   time/space dimensions
-  :ref:`unique_process` - identifies unique values for an attribute in
   results of a CQL query

Installation
------------

The above extensions are particular to the Accumulo data store.

While they can be used independently, the common use case is to use them
with GeoServer. To deploy them in GeoServer, one will require:
	a) the GeoMesa Accumulo datastore plugin
	b) the GeoServer WPS extension
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

The ``DensityProcess`` computes a density map over a set of features stored in GeoMesa. The calculations are pushed down
to the Accumulo iterators allowing for fast performance. A raster image is returned.

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

.. _statsiterator_process:

StatsIteratorProcess
^^^^^^^^^^^^^^^^^^^^

The ``StatsIteratorProcess`` allows the running of statistics on a given feature set. The statics calculations are pushed
down to the Accumulo iterators allowing for fast performance.

==========  ===========
Parameters  Description
==========  ===========
features    The feature set on which to query. Can be a raw text input, reference to a remote URL, a subquery or a vector layer.
statString  Stat string indicating which stats to instantiate. More info here :ref:`statString`.
encode      Return the values encoded as json. Must be ``true`` or ``false``; empty values will not work.
properties  The properties / transforms to apply before gathering stats. More info here :ref:`transformationsAndProjections`.
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
| ``MinMax(attribute)``                                             | * attribute*: String | Finds the Min and Max values of the given attribute.    |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| GroupBy                                                                                                                                            |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| ``GroupBy(attribute,stat)``                                       | * attribute*: String | Groups Stats by the given attribute and then runs       |
|                                                                   | * stat: Stat String  | the given stat on each group. Any stat can be provided. |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| Descriptive Stats                                                                                                                                  |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| ``DescriptiveStats(attribute)``                                   | * attribute*: String | Runs single pass stats on the given attribute           |
|                                                                   |                      | calculating stats describing the attribute such as:     |
|                                                                   |                      | Count; Min; Max; Mean; and Population and Sample        |
|                                                                   |                      | versions of Variance, Standard Deviation, Kurtosis,     |
|                                                                   |                      | Excess Kurtosis, Covariance, and Correlation.           |
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
|                                                                   | * numBins: Int       | the results into a Binned Array using the numBins as    |
|                                                                   | * lower: Int         | the number of Bins and lower and upper as the bounds    |
|                                                                   | * upper: Int         | of the Binned Array.                                    |
+-------------------------------------------------------------------+----------------------+---------------------------------------------------------+
| Freqency                                                                                                                                           |
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

.. _query_process:

QueryProcess
^^^^^^^^^^^^

The ``QueryProcess`` takes an (E)CQL query/filter for a given feature set as a text object and returns
the result as a json object. Queries are pushed to Accumulo iterators allowing for very fast performance.

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

.. _queryExampleResults:

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
	    },
	    .
	    .
	    .
	  ]
	}

.. _unique_process:

UniqueProcess
^^^^^^^^^^^^^

The ``UniqueProcess`` class is optimized for GeoMesa to find unique attributes values for a feature collection,
which are returned as a json object. Queries are pushed to Accumulo iterators allowing for very fast performance.

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
in in the 'Who' field of the Accumulo quickstart data for a restricted beounding box (-77.5, -37.5, -76.5, -36.5)). It can be run with the following curl call:

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

