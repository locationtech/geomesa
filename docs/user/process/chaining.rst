.. _chaining_processes:

Chaining Processes
^^^^^^^^^^^^^^^^^^

WPS processes can be chained together, using the result of one process as the input for another. For example, a bounding box
in a GeoMesa :ref:`query_process` can be used to restrict data sent to :ref:`stats_process`.
The following example will get all points from the AccumuloQuickStart table that are within a specified bounding box
(-77.5, -37.5, -76.5, -36.5), and calculate descriptive statistics on the 'What' attribute of the results.

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
    <ows:Identifier>geomesa:StatsIterator</ows:Identifier>
    <wps:DataInputs>
      <wps:Input>
        <ows:Identifier>features</ows:Identifier>
        <wps:Reference mimeType="text/xml" xlink:href="http://geoserver/wps" method="POST">
          <wps:Body>
            <wps:Execute version="1.0.0" service="WPS">
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
                    <wps:ComplexData mimeType="text/plain; subtype=cql"><![CDATA[BBOX(Where, -77.5, -37.5, -76.5, -36.5)]]></wps:ComplexData>
                  </wps:Data>
                </wps:Input>
              </wps:DataInputs>
              <wps:ResponseForm>
                <wps:RawDataOutput mimeType="application/json">
                  <ows:Identifier>result</ows:Identifier>
                </wps:RawDataOutput>
              </wps:ResponseForm>
            </wps:Execute>
          </wps:Body>
        </wps:Reference>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>statString</ows:Identifier>
        <wps:Data>
          <wps:LiteralData>DescriptiveStats(What)</wps:LiteralData>
        </wps:Data>
      </wps:Input>
      <wps:Input>
        <ows:Identifier>encode</ows:Identifier>
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

The query should generate results that look like this:

.. code-block:: json

  {
    "type": "FeatureCollection",
    "features": [
      {
        "type": "Feature",
        "geometry": {
          "type": "Point",
          "coordinates": [ 0, 0 ]
        },
        "properties": {
          "stats": "{\"count\":128,\"minimum\":[29.0],\"maximum\":[991.0],\"mean\":[508.5781249999999],\"population_variance\":[85116.25952148438],\"population_standard_deviation\":[291.74691004616375],\"population_skewness\":[-0.11170819256679464],\"population_kurtosis\":[1.7823482287566166],\"population_excess_kurtosis\":[-1.2176517712433834],\"sample_variance\":[85786.46628937007],\"sample_standard_deviation\":[292.893267743337],\"sample_skewness\":[-0.11303718280959842],\"sample_kurtosis\":[1.8519712064424219],\"sample_excess_kurtosis\":[-1.1480287935575781],\"population_covariance\":[85116.25952148438],\"population_correlation\":[1.0],\"sample_covariance\":[85786.46628937007],\"sample_correlation\":[1.0]}"
        },
        "id": "stat"
      }
    ]
  }
