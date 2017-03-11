<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor version="1.0.0"
  xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd"
  xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc"
  xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <NamedLayer>
    <Name>Heatmap</Name>
    <UserStyle>
      <Title>Heatmap</Title>
      <Abstract>A heatmap surface showing point density</Abstract>
      <FeatureTypeStyle>
        <Transformation>
          <ogc:Function name="gs:Heatmap">
            <ogc:Function name="parameter">
              <ogc:Literal>data</ogc:Literal>
            </ogc:Function>
            <ogc:Function name="parameter">
              <ogc:Literal>radiusPixels</ogc:Literal>
              <ogc:Function name="env">
                <ogc:Literal>radiusPixels</ogc:Literal>
                <ogc:Literal>10</ogc:Literal>
              </ogc:Function>
            </ogc:Function>
            <ogc:Function name="parameter">
              <ogc:Literal>weightAttr</ogc:Literal>
              <ogc:Function name="env">
                <ogc:Literal>weightAttr</ogc:Literal>
              </ogc:Function>
            </ogc:Function>
            <ogc:Function name="parameter">
              <ogc:Literal>outputBBOX</ogc:Literal>
              <ogc:Function name="env">
                <ogc:Literal>wms_bbox</ogc:Literal>
              </ogc:Function>
            </ogc:Function>
            <ogc:Function name="parameter">
              <ogc:Literal>outputWidth</ogc:Literal>
              <ogc:Function name="env">
                <ogc:Literal>wms_width</ogc:Literal>
              </ogc:Function>
            </ogc:Function>
            <ogc:Function name="parameter">
              <ogc:Literal>outputHeight</ogc:Literal>
              <ogc:Function name="env">
                <ogc:Literal>wms_height</ogc:Literal>
              </ogc:Function>
            </ogc:Function>
          </ogc:Function>
        </Transformation>
        <Rule>
          <RasterSymbolizer>
            <!-- specify geometry attribute to pass validation -->
            <Geometry>
              <ogc:PropertyName>the_geom</ogc:PropertyName>
            </Geometry>
            <Opacity>0.75</Opacity>
            <ColorMap type="ramp">
              <ColorMapEntry color="#0000FF" quantity="0" label="No Data"
                opacity="0" />
              <ColorMapEntry color="#00007F" quantity="0.001"
                label="Lowest" />
              <ColorMapEntry color="#0000FF" quantity="0.003"
                label="Very Low" />
              <ColorMapEntry color="#00FFFF" quantity="0.005"
                label="Low" />
              <ColorMapEntry color="#00FF00" quantity="0.01"
                label="Medium-Low" />
              <ColorMapEntry color="#FFFF00" quantity="0.02"
                label="Medium" />
              <ColorMapEntry color="#FF7F00" quantity="0.05"
                label="Medium-High" />
              <ColorMapEntry color="#FF6600" quantity="0.1"
                label="High" />
              <ColorMapEntry color="#FF0000" quantity="0.35"
                label="Very High" />
              <ColorMapEntry color="#FF00DE" quantity="0.65"
                label="Highest" />
            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>
