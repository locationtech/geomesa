package org.locationtech.geomesa.jupyter

object Jupyter {
  import com.vividsolutions.jts.geom._
  import org.opengis.feature.`type`.AttributeDescriptor
  import org.opengis.feature.simple.SimpleFeature

  trait Layer {
    def render: String
  }

  trait Shape extends Layer

  case class WMSLayer(name: String,
                      namespace: String,
                      filter: String = "INCLUDE",
                      style: String = "",
                      env: Map[String,String] = Map.empty[String,String],
                      transparent: Boolean = true,
                      ogcBaseURL: String = "http://stealthgs.ccri.com:8090/geoserver/wms") extends Layer {
    override def render: String =
      s"""
         | L.tileLayer.wms('$ogcBaseURL?',
         |   {
         |      layers: '$namespace:$name',
         |      cql_filter: "$filter",
         |      styles: '$style',
         |      env: '${env.map { case (k,v) => Array(k,v).mkString(sep = "=")}.mkString(sep = ":")}',
         |      transparent: '$transparent',
         |      format: 'image/png'
         |   }).addTo(map);
         |
       """.stripMargin
  }


  case class SimpleFeatureLayer(features: Seq[SimpleFeature]) extends Layer {
    override def render: String =
      s"""
         |L.geoJson(${features.map(simpleFeatureToGeoJSON).mkString("[",",","]")},
         |{
         |    "color": "#ff7800",
         |    "weight": 5,
         |    "opacity": 0.65,
         |    "onEachFeature": geojsonPopup
         |}).addTo(map);
       """.stripMargin

    private def simpleFeatureToGeoJSON(sf: SimpleFeature) = {
      import scala.collection.JavaConversions._
      s"""
         |{
         |    "type": "Feature",
         |    "properties": {
         |        ${sf.getType.getAttributeDescriptors.zip(sf.getAttributes).map { case (d, a) => propToJson(d, a) }.mkString(sep =",\n")}
         |    },
         |    "geometry": {
         |        "type": "${sf.getDefaultGeometry.asInstanceOf[Geometry].getGeometryType}",
         |        "coordinates": ${processGeometry(sf.getDefaultGeometry.asInstanceOf[Geometry])}
         |    }
         |}
         |
       """.stripMargin
    }

    private def propToJson(d: AttributeDescriptor, a: Object) = s"${d.getLocalName}: '${java.net.URLEncoder.encode(a.toString, "UTF-8")}'"

    private def processGeometry(geom: Geometry) = geom.getGeometryType match {
      case "Point"      => processPoint(geom.asInstanceOf[Point])
      case "LineString" => processLinestring(geom.asInstanceOf[LineString])
      case "Polygon"    => processPoly(geom.asInstanceOf[Polygon])
    }

    def processCoord(c: Coordinate) = s"[${c.x}, ${c.y}]"
    def processPoint(p: Point) = s"${processCoord(p.getCoordinate)}"
    def processLinestring(l: LineString) = s"[${l.getCoordinates.map { c =>processCoord(c)}.mkString(",")}]"
    def processPoly(p: Polygon) = s"[${p.getCoordinates.map { c =>processCoord(c)}.mkString(",")}]"
  }

  case class StyleOptions(color: String, fillColor: String, fillOpacity: Double) {
    def render: String =
      s"""
         |{
         |   color:       '$color',
         |   fillColor:   '$fillColor',
         |   fillOpacity: $fillOpacity
         |}
       """.stripMargin
  }


  case class Circle(cx: Double, cy: Double, radiusMeters: Double, style: StyleOptions) extends Layer {
    override def render: String =
      s"""
         |L.circle([$cy, $cx], $radiusMeters, ${style.render}).addTo(map);
       """.stripMargin
  }

  implicit class JTSPolyLayer(val poly: Polygon) extends Layer {
    override def render: String = ??? //TODO
  }

  implicit class JTSPointLayer(val point: Point) extends Layer {
    override def render: String = ??? //TODO
  }

  implicit class JTSLineStringLayer(val ls: LineString) extends Layer {
    override def render: String = ??? //TODO
  }
}


trait Leaflet {

  import Jupyter._

  def html(layers: Seq[Layer]) =
    s"""
       |<html>
       |<head>
       | <link rel="stylesheet" href="https://unpkg.com/leaflet@1.0.0-rc.3/dist/leaflet.css" />
       | <script src="https://unpkg.com/leaflet@1.0.0-rc.3/dist/leaflet.js"></script>
       |</head>
       |
       |<body>
       |<div id='map' style="width:100%;height:600px"/>
       |<script type="text/javascript">
       |
       |   function jsonToTable(props) {
       |       return decodeURI(props.text);
       |   };
       |
       |   function geojsonPopup(feature, layer) {
       |       if (feature.properties) {
       |           layer.bindPopup(jsonToTable(feature.properties));
       |       }
       |   };
       |
       |   var baseLayer = L.tileLayer('http://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png',{
       |     attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, &copy; <a href="https://carto.com/attributions">CARTO</a>'
       |   });
       |
       |   var map = L.map('map', {
       |     scrollWheelZoom: false,
       |     center: [40.7127837, -74.0059413],
       |     zoom: 6
       |   });
       |
       |   map.addLayer(baseLayer);
       |
       |   ${layers.map(_.render).mkString(sep = "\n")}
       |</script>
       |</body>
       |</html>
     """.stripMargin


  def render(layers: Seq[Layer]) = frame("my-frame", html(layers))

  private def frame(frameName: String, body: String) = {
    s"""
       |  <iframe id="$frameName"
       |     sandbox="allow-scripts allow-same-origin"
       |     style="border: none; width: 100%"
       |     srcdoc="${xml.Utility.escape(body)}"></iframe>
       |  <script>
       |    if (typeof resizeIFrame != 'function') {
       |      function resizeIFrame(el, k) {
       |        el.style.height = el.contentWindow.document.body.scrollHeight + 'px';
       |        if (k <= 3) { setTimeout(function() { resizeIFrame(el, k+1) }, 1000) };
       |      }
       |    }
       |    $$().ready( function() { resizeIFrame($$('#$frameName').get(0), 1); });
       |  </script>
    """.stripMargin
  }
}

object L extends Leaflet
