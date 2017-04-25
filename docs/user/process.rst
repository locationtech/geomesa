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
-  :ref:`QueryProcess` - performs a Geomesa optimized query using spatiotemporal indexes
-  ``sampling_process`` - uses statistical sampling to reduces the features
   returned by a query
-  :ref:`StatsIteratorProcess` - returns various stats for a CQL query
-  ``TubeSelectProcess`` - performs a correlated search across
   time/space dimensions
-  :ref:`UniqueProcess` - identifies unique values for an attribute in
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
^^^^^^^^^^^^^^^^^^^^

The ``QueryProcess`` takes an (E)CQL query/filter for a given feature set as a text object and returns
the result as a json object. Queries are pushed to Accumulo iterators allowing for very fast performance.

==========  ===========
Parameters  Description
==========  ===========
features    The data source feature collection to query. Reference as store:layername.
		For an XML file enter <wfs:Query typeName=store:layername />
		For interactive WPS request builder select VECTOR_LAYER & choose store:layername

filter      The filter to apply to the feature collection.
		For an XML file enter:
			<wps:ComplexData mimeType="text/plain; subtype=cql"><![CDATA[some-query-text]]</wps:ComplexData>
		For interactive WPS request builder select TEXT & choose "text/plain; subtype=cql"
			enter the query text in the text box

output      Specify how the output feature collection will be presented.
		For an XML file enter:
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
""""""""""""

Below is an example of a geoserver WPS call to the GeoMesa QueryProcess that performs the same query shown
in the 'Accumulo-quickstart <http://www.geomesa.org/documentation/tutorials/geomesa-quickstart-accumulo.html/>'_. It can be saved as QueryProcess_wps.xml and run with the following curl call:

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@QueryProcess_wps.xml localhost:8080/geoserver/wps

<?xml version="1.0" encoding="UTF-8"?>
<wps:Execute version="1.0.0" service="WPS" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.opengis.net/wps/1.0.0"
    xmlns:wfs="http://www.opengis.net/wfs" xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1"
    xmlns:gml="http://www.opengis.net/gml" xmlns:ogc="http://www.opengis.net/ogc" xmlns:wcs="http://www.opengis.net/wcs/1.1.1"
    xmlns:xlink="http://www.w3.org/1999/xlink" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsAll.xsd">
  <ows:Identifier>geomesa:Query</ows:Identifier>
  <wps:DataInputs>
    <wps:Input>
      <ows:Identifier>features</ows:Identifier>
      <wps:Reference mimeType="text/xml" xlink:href="http://geoserver/wfs" method="POST">
        <wps:Body>
          <wfs:GetFeature service="WFS" version="1.0.0" outputFormat="GML2" xmlns:cite="http://www.opengeospatial.net/cite">
            <wfs:Query typeName="cite:AccumuloQuickStart"/>
          </wfs:GetFeature>
        </wps:Body>
      </wps:Reference>
    </wps:Input>
    <wps:Input>
      <ows:Identifier>filter</ows:Identifier>
      <wps:Data>
        <wps:ComplexData mimeType="text/plain; subtype=cql"><![CDATA[BBOX(Where, -77.5, -37.5, -76.5, -36.5)
AND (Who = 'Bierce')
AND (When DURING 2014-07-01T00:00:00.000Z/2014-09-30T23:59:59.999Z)]]></wps:ComplexData>
      </wps:Data>
    </wps:Input>
  </wps:DataInputs>
  <wps:ResponseForm>
    <wps:RawDataOutput mimeType="application/json">
      <ows:Identifier>result</ows:Identifier>
    </wps:RawDataOutput>
  </wps:ResponseForm>
</wps:Execute>
^^^^^^^^^^^^

.. _queryExampleResults:

Example results
""""""""""""

The query should generate results that look like this:

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
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [
          -76.8815,
          -37.4016
        ]
      },
      "properties": {
        "Who": "Bierce",
        "What": 589,
        "When": "2014-07-05T06:02:15.000+0000"
      },
      "id": "Observation.589"
    },
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [
          -76.598,
          -37.1842
        ]
      },
      "properties": {
        "Who": "Bierce",
        "What": 886,
        "When": "2014-07-22T18:12:36.000+0000"
      },
      "id": "Observation.886"
    },
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [
          -77.0176,
          -37.3093
        ]
      },
      "properties": {
        "Who": "Bierce",
        "What": 322,
        "When": "2014-07-15T21:09:42.000+0000"
      },
      "id": "Observation.322"
    },
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [
          -76.5621,
          -37.3432
        ]
      },
      "properties": {
        "Who": "Bierce",
        "What": 925,
        "When": "2014-08-18T03:28:33.000+0000"
      },
      "id": "Observation.925"
    },
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [
          -77.4256,
          -37.2671
        ]
      },
      "properties": {
        "Who": "Bierce",
        "What": 394,
        "When": "2014-08-01T23:55:05.000+0000"
      },
      "id": "Observation.394"
    },
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [
          -76.6683,
          -37.445
        ]
      },
      "properties": {
        "Who": "Bierce",
        "What": 343,
        "When": "2014-08-06T08:59:22.000+0000"
      },
      "id": "Observation.343"
    },
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [
          -76.9012,
          -37.1485
        ]
      },
      "properties": {
        "Who": "Bierce",
        "What": 259,
        "When": "2014-08-28T19:59:30.000+0000"
      },
      "id": "Observation.259"
    },
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [
          -77.3622,
          -37.1301
        ]
      },
      "properties": {
        "Who": "Bierce",
        "What": 640,
        "When": "2014-09-14T19:48:25.000+0000"
      },
      "id": "Observation.640"
    }
  ]
}

