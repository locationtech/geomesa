HashAttributeProcess
^^^^^^^^^^^^^^^^^^^^

The ``HashAttributeProcess`` adds an attribute to each SimpleFeature that hashes the configured attribute modulo the configured
param. This can be used for layer styling. It accepts the following parameters:

============  ===========
Parameter     Description
============  ===========
data          Input Simple Feature Collection to run the hash process over
attribute     The attribute to hash on
modulo        The divisor
============  ===========

HashAttributeColorProcess
-------------------------

The ``HashAttributeColorProcess`` extends the ``HashAttributeProcess``, but sets a hex color string instead of an int, which
can simplify layer styling.

Hash Example
------------

The following XML file shows an example of the HashAttributeProcess:

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
    <ows:Identifier>geomesa:HashAttribute</ows:Identifier>
    <wps:DataInputs>
      <wps:Input>
        <ows:Identifier>data</ows:Identifier>
        <wps:Reference mimeType="text/xml" xlink:href="http://geoserver/wfs" method="POST">
          <wps:Body>
            <wfs:GetFeature service="WFS" version="1.0.0" outputFormat="GML2" xmlns:cabs="cabs">
              <wfs:Query typeName="cabs:Cabs_features"/>
            </wfs:GetFeature>
          </wps:Body>
        </wps:Reference>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>attribute</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>CabId</wps:LiteralData>
        </wps:Data>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>modulo</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>256</wps:LiteralData>
        </wps:Data>
      </wps:Input>
    </wps:DataInputs>
    <wps:ResponseForm>
      <wps:RawDataOutput mimeType="application/json">
        <ows:Identifier>result</ows:Identifier>
      </wps:RawDataOutput>
    </wps:ResponseForm>
  </wps:Execute>

Assuming the above XML is in a file named ``HashAttributeProcess_wps.xml``, it can be invoked with the following ``curl`` to
GeoServer:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@HashAttributeProcess_wps.xml localhost:8080/geoserver/wps


The query should generate results that look like this:

.. code-block:: json

  {
    "type" : "FeatureCollection",
    "features": [
      {
        "id": "d0971735-f8fe-47ed-a7cd-2e12280e8ac1",
        "geometry": {
          "coordinates": [ 151.1554, 18.2014 ],
          "type": "Point"
        },
        "type": "Feature",
        "properties": {
          "Vitesse": 614,
          "Heading": 244,
          "Date": "2016-05-02T18:00:44.030+0000",
          "CabId": 150002,
          "hash": 237
        }
      }
    ]
  }
