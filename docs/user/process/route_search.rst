RouteSearchProcess
^^^^^^^^^^^^^^^^^^

The ``RouteSearchProcess`` finds features traveling along a route (and not just crossing over it). It accepts the following
parameters:

================  ===========
Parameter         Description
================  ===========
features          Input feature collection to query
routes            Routes to search along. Features must have a geometry of LineString
bufferSize        Buffer size (in meters) to search around the route
headingThreshold  Threshold for comparing headings, in degrees
routeGeomField    Attribute that will be examined for routes to match. Must be a LineString
geomField         Attribute that will be examined for route matching
bidirectional     Consider the direction of the route or just the path of the route
headingField      Attribute that will be examined for heading in the input features. If not provided, input features geometries must be LineStrings
================  ===========

Route Search Example
--------------------

The following XML is an example of the RouteSearchProcess:

.. code-block:: xml

  <?xml version="1.0" encoding="UTF-8"?>
  <wps:Execute version="1.0.0"
      service="WPS"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xmlns="http://www.opengis.net/wps/1.0.0"
      xmlns:wfs="http://www.opengis.net/wfs"
      xmlns:wps="http://www.opengis.net/wps/1.0.0"
      xmlns:ows="http://www.opengis.net/ows/1.1"
      xmlns:xlink="http://www.w3.org/1999/xlink"
      xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsAll.xsd">
    <ows:Identifier>geomesa:RouteSearch</ows:Identifier>
    <wps:DataInputs>
      <wps:Input>
        <ows:Identifier>features</ows:Identifier>
        <wps:Reference mimeType="text/xml" xlink:href="http://geoserver/wfs" method="POST">
          <wps:Body>
            <wfs:GetFeature service="WFS" version="1.0.0" outputFormat="GML2" xmlns:cabs="cabs">
              <wfs:Query typeName="cabs:Cabs_features"/>
            </wfs:GetFeature>
          </wps:Body>
        </wps:Reference>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>routes</ows:Identifier>
        <wps:Reference mimeType="text/xml" xlink:href="http://geoserver/wfs" method="POST">
          <wps:Body>
            <wfs:GetFeature service="WFS" version="1.0.0" outputFormat="GML2" xmlns:cabs="cabs">
              <wfs:Query typeName="cabs:Routes"/>
            </wfs:GetFeature>
          </wps:Body>
        </wps:Reference>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>bufferSize</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>50</wps:LiteralData>
        </wps:Data>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>headingThreshold</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>10</wps:LiteralData>
        </wps:Data>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>routeGeomField</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>Route</wps:LiteralData>
        </wps:Data>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>geomField</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>Position</wps:LiteralData>
        </wps:Data>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>bidirectional</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>true</wps:LiteralData>
        </wps:Data>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>headingField</ows:Identifier>
        <wps:Data>
        </wps:Data>
      </wps:Input>
    </wps:DataInputs>
    <wps:ResponseForm>
      <wps:RawDataOutput mimeType="application/json">
        <ows:Identifier>result</ows:Identifier>
      </wps:RawDataOutput>
    </wps:ResponseForm>
  </wps:Execute>

Assuming the above XML is in a file named ``RouteSearchProcess_wps.xml``, it can be invoked with the following ``curl`` to
GeoServer:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@RouteSearchProcess_wps.xml localhost:8080/geoserver/wps
