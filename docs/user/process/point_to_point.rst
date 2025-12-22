Point2PointProcess
^^^^^^^^^^^^^^^^^^

The ``Point2PointProcess`` aggregates a collection of points into a collection of line segments. It accepts the following
parameters:

=====================  ===========
Parameter              Description
=====================  ===========
data                   Input feature collection
groupingField          Field on which to group
sortField              Field on which to sort (must be Date type)
minimumNumberOfPoints  Minimum number of points
breakOnDay             Break connections on day marks
filterSingularPoints   Filter out segments that fall on the same point
=====================  ===========

Point2Point Example
-------------------

The following XML shows an example Point2Point process call:

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
    <ows:Identifier>geomesa:Point2Point</ows:Identifier>
    <wps:DataInputs>
      <wps:Input>
        <ows:Identifier>data</ows:Identifier>
        <wps:Reference mimeType="text/xml" xlink:href="http://geoserver/wps" method="POST">
          <wps:Body>
            <wfs:GetFeature service="WFS" version="1.0.0" outputFormat="GML2" xmlns:cabs="cabs">
              <wfs:Query typeName="cabs:Cabs_features"/>
            </wfs:GetFeature>
          </wps:Body>
        </wps:Reference>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>groupingField</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>CabId</wps:LiteralData>
        </wps:Data>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>sortField</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>Date</wps:LiteralData>
        </wps:Data>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>minimumNumberOfPoints</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>3</wps:LiteralData>
        </wps:Data>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>breakOnDay</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>false</wps:LiteralData>
        </wps:Data>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>filterSingularPoints</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>true</wps:LiteralData>
        </wps:Data>
      </wps:Input>
    </wps:DataInputs>
    <wps:ResponseForm>
      <wps:RawDataOutput mimeType="application/json">
        <ows:Identifier>result</ows:Identifier>
      </wps:RawDataOutput>
    </wps:ResponseForm>
  </wps:Execute>

Assuming the above XML is in a file named ``Point2PointProcess_wps.xml``, it can be invoked with the following ``curl`` to GeoServer:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@Point2PointProcess_wps.xml localhost:8080/geoserver/wps

The query should generate results that look like this:

.. code-block:: json

  {
    "type" : "FeatureCollection",
    "features": [
      {
        "id": "367152240-4",
        "geometry": {
          "coordinates": [ [ -13.4041, 37.8067 ], [ -13.4041, 37.8068 ] ],
          "type": "LineString"
        },
        "type": "Feature",
        "properties": {
          "Date_end": "2018-02-05T14:54:36.598+0000",
          "CabId": 367152240,
          "Date_start": "2018-02-05T14:53:58.078+0000"
        }
      }
    ]
  }
