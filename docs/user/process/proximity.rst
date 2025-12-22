ProximitySearchProcess
^^^^^^^^^^^^^^^^^^^^^^

The ``ProximitySearchProcess`` performs a proximity search on a Geomesa feature collection using another feature collection as
input. It accepts the following parameters:

=====================  ===========
Parameter              Description
=====================  ===========
inputFeatures          Input feature collection that defines the proximity search
dataFeatures           The data set to query for matching features
bufferDistance         Buffer size in meters
=====================  ===========

Proximity Search Example
------------------------

The following XML is an example of the ProximitySearchProcess:

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
    <ows:Identifier>geomesa:ProximitySearch</ows:Identifier>
    <wps:DataInputs>
      <wps:Input>
        <ows:Identifier>inputFeatures</ows:Identifier>
        <wps:Reference mimeType="text/xml; subtype=wfs-collection/1.0" xlink:href="http://geoserver/wps" method="POST">
          <wps:Body>
            <wps:Execute version="1.0.0" service="WPS">
              <ows:Identifier>geomesa:Query</ows:Identifier>
              <wps:DataInputs>
                <wps:Input>
                  <ows:Identifier>features</ows:Identifier>
                  <wps:Reference mimeType="text/xml; subtype=wfs-collection/1.0" xlink:href="http://geoserver/wfs" method="POST">
                    <wps:Body>
                      <wfs:GetFeature service="WFS" version="1.0.0" outputFormat="GML2" xmlns:cabs="cabs">
                        <wfs:Query typeName="cabs:Cabs_features"/>
                      </wfs:GetFeature>
                    </wps:Body>
                  </wps:Reference>
                </wps:Input>
                <wps:Input>
                  <ows:Identifier>filter</ows:Identifier>
                  <wps:Data>
                    <wps:ComplexData mimeType="text/plain; subtype=cql"><![CDATA[CabId = 367152240]]></wps:ComplexData>
                  </wps:Data>
                </wps:Input>
              </wps:DataInputs>
              <wps:ResponseForm>
                <wps:RawDataOutput mimeType="text-xml; subtype=wfs-collection/1.0">
                  <ows:Identifier>result</ows:Identifier>
                </wps:RawDataOutput>
              </wps:ResponseForm>
            </wps:Execute>
          </wps:Body>
        </wps:Reference>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>dataFeatures</ows:Identifier>
        <wps:Reference mimeType="text/xml; subtype=wfs-collection/1.0" xlink:href="http://geoserver/wps" method="POST">
          <wps:Body>
            <wps:Execute version="1.0.0" service="WPS">
              <ows:Identifier>geomesa:Query</ows:Identifier>
              <wps:DataInputs>
                <wps:Input>
                  <ows:Identifier>features</ows:Identifier>
                  <wps:Reference mimeType="text/xml; subtype=wfs-collection/1.0" xlink:href="http://geoserver/wfs" method="POST">
                    <wps:Body>
                      <wfs:GetFeature service="WFS" version="1.0.0" outputFormat="GML2" xmlns:cabs="cabs">
                        <wfs:Query typeName="cabs:Cabs_features"/>
                      </wfs:GetFeature>
                    </wps:Body>
                  </wps:Reference>
                </wps:Input>
                <wps:Input>
                  <ows:Identifier>filter</ows:Identifier>
                  <wps:Data>
                    <wps:ComplexData mimeType="text/plain; subtype=cql"><![CDATA[NOT(CabId = 367152240)]]></wps:ComplexData>
                  </wps:Data>
                </wps:Input>
              </wps:DataInputs>
              <wps:ResponseForm>
                <wps:RawDataOutput mimeType="text-xml; subtype=wfs-collection/1.0">
                  <ows:Identifier>result</ows:Identifier>
                </wps:RawDataOutput>
              </wps:ResponseForm>
            </wps:Execute>
          </wps:Body>
        </wps:Reference>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>bufferDistance</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>20</wps:LiteralData>
        </wps:Data>
      </wps:Input>
    </wps:DataInputs>
    <wps:ResponseForm>
      <wps:RawDataOutput mimeType="application/json">
        <ows:Identifier>result</ows:Identifier>
      </wps:RawDataOutput>
    </wps:ResponseForm>
  </wps:Execute>

Assuming the above XML is in a file named ``ProximitySearchProcess_wps.xml``, it can be invoked with the following ``curl`` to
GeoServer:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@ProximitySearchProcess_wps.xml localhost:8080/geoserver/wps
