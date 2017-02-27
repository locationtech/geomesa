* A sample notebook session

```scala
   classpath.addRepository("http://download.osgeo.org/webdav/geotools")
   classpath.addRepository("http://central.maven.org/maven2")
   classpath.addRepository("https://repo.locationtech.org/content/repositories/geomesa-releases")
   classpath.addRepository("file:///home/username/.m2/repository")
   classpath.add("com.vividsolutions" % "jts" % "1.13")
   classpath.add("org.locationtech.geomesa" % "geomesa-accumulo-datastore" % "1.3.0")
   classpath.add("org.apache.accumulo" % "accumulo-core" % "1.6.4")
   classpath.add("org.locationtech.geomesa" % "geomesa-jupyter" % "1.3.0")
   
   import org.locationtech.geomesa.jupyter.Jupyter._
   
   implicit val displayer: String => Unit = display.html(_)
   
   import scala.collection.JavaConversions._
   import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams._
   import org.locationtech.geomesa.utils.geotools.Conversions._
   
   val params = Map(
           zookeepersParam.key -> "ZOOKEEPERS",
           instanceIdParam.key -> "INSTANCE",
           userParam.key       -> "USER_NAME",
           passwordParam.key   -> "USER_PASS",
           tableNameParam.key  -> "TABLENAME")
   
   val ds = org.geotools.data.DataStoreFinder.getDataStore(params)
   val ff = org.geotools.factory.CommonFactoryFinder.getFilterFactory2
   val fs = ds.getFeatureSource("twitter")
   
   val filt = ff.and(
       ff.between(ff.property("dtg"), ff.literal("2016-01-01"), ff.literal("2016-05-01")), 
       ff.bbox("geom", -80, 37, -75, 40, "EPSG:4326"))
   val features = fs.getFeatures(filt).features.take(10).toList
   
   displayer(L.render(Seq(WMSLayer(name="ne_10m_roads",namespace="NAMESPACE"),
                          Circle(-78.0,38.0,1000,  StyleOptions(color="yellow",fillColor="#63A",fillOpacity=0.5)),
                          Circle(-78.0,45.0,100000,StyleOptions(color="#0A5" ,fillColor="#63A",fillOpacity=0.5)),
                          SimpleFeatureLayer(features)
                         )))
   

```

![ScreenShot](assets/Jupyter.png)

## Adding Layers to a map and displaying in notebook

The snippet below is an example of rendering dataframes in Leaflet in a Jupyter notebook.

```scala

  implicit val displayer: String => Unit = { s => kernel.display.content("text/html", s) }
  
  val function = """
    function(feature) {
      switch (feature.properties.plane_type) {
        case "A388": return {color: "#1c2957"}
        default: return {color: "#cdb87d"}
      }
    }
  """
  
  val sftLayer = time { L.DataFrameLayerNonPoint(flights_over_state, "__fid__", L.StyleOptionFunction(function)) }
  val apLayer = time { L.DataFrameLayerPoint(flyovers, "origin", L.StyleOptions(color="#1c2957", fillColor="#cdb87d"), 2.5) }
  val stLayer = time { L.DataFrameLayerNonPoint(queryOnStates, "ST", L.StyleOptions(color="#1c2957", fillColor="#cdb87d", fillOpacity= 0.45)) }
  displayer(L.render(Seq[L.GeoRenderable](sftLayer,stLayer,apLayer),zoom = 1, path = "path/to/files"))
```

![ScreenShot](assets/Leaflet.png)

### StyleOptionFunction
This case class allows you to specify a Javascript function to perform styling. The anonymous function
that you will pass takes a feature as an argument and returns a Javascript style object. An example of styling
based on a specific property value is provided below:

```javascript
   function(feature) { 
     switch(feature.properties.someProp) {
       case "someValue": return { color: "#ff0000" }
       default         : return { color: "#0000ff" }
     }
   }
```

The following table provides options that might be of interest:


| Option       | Type        | Description            |
| ------------ | ----------- | ---------------------- |
| color        | String      | Stroke color           |
| weight       | Number      | Stroke width in pixels |
| opacity      | Number      | Stroke opacity         |
| fillColor    | String      | Fill color             |
| fillOpacity  | Number      | Fill opacity           |
 
Note: Options are comma-separated (i.e. ```{ color: "#ff0000", fillColor: "#0000ff" }```)
