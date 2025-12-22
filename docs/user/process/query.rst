.. _query_process:

QueryProcess
^^^^^^^^^^^^

The ``QueryProcess`` takes a CQL query/filter for a given feature set as a text object and returns the results. It accepts the
following parameters:

==========  ===========
Parameter   Description
==========  ===========
features    The layer to query.
filter      The filter to apply to the layer.
properties  The properties/transforms to return in the response.
output      The response format.
==========  ===========

Query Example
-------------

The following XML is an example of the QueryProcess:

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

Assuming the above XML is in a file named ``QueryProcess_wps.xml``, it can be invoked with the following ``curl`` to GeoServer:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@QueryProcess_wps.xml localhost:8080/geoserver/wps

The query should generate results that look like:

.. code-block:: json

    {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "geometry": {
            "type": "Point",
            "coordinates": [ -76.513, -37.4941 ]
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
