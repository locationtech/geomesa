TrackLabelProcess
^^^^^^^^^^^^^^^^^

The ``TrackLabelProcess`` returns a single feature that is the head of a track of related simple features, useful for styling.
It accepts the following parameters:

==========  ===========
Parameter   Description
==========  ===========
data        Input features
track       Track attribute to use for grouping features
dtg         Date attribute to use for ordering tracks
==========  ===========

TrackLabel Example
------------------

The following XML is an example of the TrackLabelProcess:

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
    <ows:Identifier>geomesa:TrackLabel</ows:Identifier>
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
        <ows:Identifier>track</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>CabId</wps:LiteralData>
        </wps:Data>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>dtg</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>Date</wps:LiteralData>
        </wps:Data>
      </wps:Input>
    </wps:DataInputs>
    <wps:ResponseForm>
      <wps:RawDataOutput mimeType="application/json">
        <ows:Identifier>result</ows:Identifier>
      </wps:RawDataOutput>
    </wps:ResponseForm>
  </wps:Execute>

Assuming the above XML is in a file named ``TrackLabelProcess_wps.xml``, it can be invoked with the following ``curl`` to
GeoServer:

.. code-block:: bash

    curl -v -u admin:geoserver -H "Content-Type: text/xml" -d@TrackLabelProcess_wps.xml localhost:8080/geoserver/wps
