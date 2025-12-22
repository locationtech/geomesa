Request Parameters
^^^^^^^^^^^^^^^^^^

WPS requests must typically be encoded as XML; however, the GeoServer documentation is not always clear about exactly what XML
is required. This page explains a few different parameter types, with examples of how to represent them.

Selecting Layers
----------------

The primary input of a process is usually a feature collection, representing a layer in GeoServer. A layer can be selected as:

.. code-block:: xml

  <wps:Input>
     <!-- name of the input parameter from the process definition -->
    <ows:Identifier>features</ows:Identifier>
    <wps:Reference mimeType="text/xml; subtype=wfs-collection/1.0" xlink:href="http://geoserver/wfs" method="POST">
      <wps:Body>
        <wfs:GetFeature service="WFS" version="1.0.0" outputFormat="GML2" xmlns:cabs="cabs">
          <!-- typeName is workspace:layer -->
          <wfs:Query typeName="cabs:Cabs_features"/>
        </wfs:GetFeature>
      </wps:Body>
    </wps:Reference>
  </wps:Input>

CQL Filters
-----------

CQL filters can be represented as complex XML elements, but it's often easier to represent them as text strings:

.. code-block:: xml

  <wps:Input>
    <!-- name of the input parameter from the process definition -->
    <ows:Identifier>filter</ows:Identifier>
    <wps:Data>
      <wps:ComplexData mimeType="text/plain; subtype=cql">
        <!-- filter string, wrapped in a CDATA field to allow special characters like &lt; and &gt; -->
        <![CDATA[INCLUDE]]>
      </wps:ComplexData>
    </wps:Data>
  </wps:Input>

In the GeoServer interactive WPS request builder, select ``TEXT`` and choose ``"text/plain; subtype=cql"``, then enter the
query text in the text box.

Other Inputs
------------

Most other inputs (string, integers, Booleans, etc) can be represented as literal data:

.. code-block:: xml

  <wps:Input>
    <!-- name of the input parameter from the process definition -->
    <ows:Identifier>histogram</ows:Identifier>
    <wps:Data>
      <wps:LiteralData>true</wps:LiteralData>
    </wps:Data>
  </wps:Input>

Output Formats
--------------

Responses can be rendered in any output format supported by GeoServer; common ones are GeoJSON and GML:

.. code-block:: xml

  <!-- GeoJSON -->
  <wps:ResponseForm>
     <wps:RawDataOutput mimeType="application/json">
        <ows:Identifier>result</ows:Identifier>
     </wps:RawDataOutput>
  </wps:ResponseForm>

.. code-block:: xml

  <!-- GML -->
  <wps:ResponseForm>
    <wps:RawDataOutput mimeType="text-xml; subtype=wfs-collection/1.0">
      <ows:Identifier>result</ows:Identifier>
    </wps:RawDataOutput>
  </wps:ResponseForm>

In the GeoServer interactive WPS request builder, select ``VECTOR_LAYER``, then choose the layer.
