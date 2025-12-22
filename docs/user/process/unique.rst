UniqueProcess
^^^^^^^^^^^^^

The ``UniqueProcess`` is optimized for GeoMesa to find unique attributes values for a feature collection,
which are returned as a json object. It accepts the following parameters:

===========  ===========
Parameter    Description
===========  ===========
features     The layer to query
attribute    The attribute for which unique values will be extracted
filter       A filter to apply to the feature collection
histogram    A Boolean to enable returning a histogram of attribute values, instead of just a count of the total unique values
sort         Sort the results; allowed values are ``ASC`` or ``DESC``
sortByCount  A Boolean to enable sorting by histogram counts instead of attribute values
output       The output feature collection encoding
===========  ===========

Unique Example
--------------

The following XML is an example of a UniqueProcess that reports the unique names in in the 'Who' field of a layer over a
restricted bounding box (-77.5, -37.5, -76.5, -36.5):

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
    <ows:Identifier>geomesa:Unique</ows:Identifier>
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
        <ows:Identifier>attribute</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>Who</wps:LiteralData>
        </wps:Data>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>filter</ows:Identifier>
        <wps:Data>
          <wps:ComplexData mimeType="text/plain; subtype=cql"><![CDATA[BBOX(Where, -77.5, -37.5, -76.5, -36.5)]]></wps:ComplexData>
        </wps:Data>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>histogram</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>true</wps:LiteralData>
        </wps:Data>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>sort</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>ASC</wps:LiteralData>
        </wps:Data>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>sortByCount</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>false</wps:LiteralData>
        </wps:Data>
      </wps:Input>
    </wps:DataInputs>
    <wps:ResponseForm>
      <wps:RawDataOutput mimeType="application/json">
        <ows:Identifier>result</ows:Identifier>
      </wps:RawDataOutput>
    </wps:ResponseForm>
  </wps:Execute>

Assuming the above XML is in a file named ``UniqueProcess_wps.xml``, it can be invoked with the following ``curl`` to GeoServer:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@UniqueProcess_wps.xml localhost:8080/geoserver/wps

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
