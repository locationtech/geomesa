/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jupyter

object L {
  import org.apache.commons.text.CharacterPredicates.ASCII_ALPHA_NUMERALS
  import org.apache.commons.text.{RandomStringGenerator, StringEscapeUtils}
  import org.apache.spark.sql._
  import org.geotools.api.feature.`type`.AttributeDescriptor
  import org.geotools.api.feature.simple.SimpleFeature
  import org.locationtech.geomesa.spark.sql.SparkUtils
  import org.locationtech.jts.geom._

  trait GeoRenderable {
    def render: String
  }

  trait Shape extends GeoRenderable

  trait StyleOption {
    def render: String
  }

  case class StyleOptions(color: String = "#000000", fillColor: String = "#327A66", fillOpacity: Double = 0.75)
    extends StyleOption {
    def render: String =
      s"""
         |{
         |  color: '$color',
         |  fillColor: '$fillColor',
         |  fillOpacity: '$fillOpacity'
         |}
       """.stripMargin
  }

  /**
    * Takes a string containing a valid javascript styling function as its argument
    * @param javascriptFunction The javascript styling function
    */
  case class StyleOptionFunction(javascriptFunction: String) extends StyleOption {
    def render: String = javascriptFunction
  }


  private object OnFeatureClick {
    def render: String =
      """
       |function onClick(e) {
       |  e.target.openPopup();
       |}
       |
       |function onFeature(feature, layer) {
       |  var keys = Object.keys(layer.feature.properties);
       |  var str = "";
       |  for (var i in keys) {
       |    var key = keys[i];
       |    var prop = layer.feature.properties[key];
       |    str = str + "<b>" + key + "</b>: " + prop;
       |    if (i < keys.length - 1 ) str = str + "<br>";
       |  }
       |  layer.bindPopup(str);
       |  layer.on({click: onClick});
       |}
     """.stripMargin
  }

  private case class PointToLayer(style: StyleOption, radius: Double = 5.0) {
    val styleOptions = style.render
    def render: String =
      s"""
        |pointToLayer: function(feature, latlng) {
        |  return L.circleMarker(latlng, {
        |                                   radius: $radius,
        |                                   ${styleOptions.replace("}", "").replace("{", "")}
        |                                  });
        |}
      """.stripMargin
  }

  case class WMSLayer(layerName: String,
                      style: String = "",
                      filter: String = "INCLUDE",
                      color: String = "#FF0000",
                      geoserverURL: String = "/geoserver/wms",
                      env: Map[String,String] = Map.empty[String,String],
                      opacity: Double = 0.6,
                      transparent: Boolean = true) extends GeoRenderable {
    override def render: String =
      s"""
         | L.WMS.source('$geoserverURL?',
         |   {
         |      layers: '$layerName',
         |      cql_filter: "$filter",
         |      styles: '$style',
         |      env: '${env.map { case (k, v) => Array(k, v).mkString(sep = "=") }.mkString(sep = ":")}',
         |      transparent: '$transparent',
         |      opacity: $opacity,
         |      format: 'image/png',
         |      version: '1.1.1'
         |   }).getLayer('$layerName').addTo(map);
         |
       """.stripMargin
  }

  trait DataFrameLayer {
    def df: DataFrame
    def idField: String
    def style: StyleOption
  }

  trait SimpleFeatureLayer {
    private val writer = new org.locationtech.jts.io.geojson.GeoJsonWriter()
    writer.setEncodeCRS(false)

    def features: Seq[SimpleFeature]
    def style: StyleOption

    def simpleFeatureToGeoJSON(sf: SimpleFeature) = {
      import scala.collection.JavaConverters._
      s"""
         |{
         |    "type": "Feature",
         |    "properties": {
         |        ${sf.getType.getAttributeDescriptors.asScala.zip(sf.getAttributes.asScala)
                      .filter{case(a,b) => b != null && !b.isInstanceOf[Geometry]}
                      .map { case (d, a) => propToJson(d, a) }.mkString(sep =",\n")
                   }
         |    },
         |    "geometry": ${writer.write(sf.getDefaultGeometry.asInstanceOf[Geometry])}
         |}
       """.stripMargin

    }

    def propToJson(ad: AttributeDescriptor, a: Object) =
      if(a!= null) s""""${ad.getLocalName}": "${StringEscapeUtils.escapeJson(a.toString)}""""
      else s""""${ad.getLocalName}": ''"""
  }

  case class DataFrameLayerNonPoint(df: DataFrame, idField: String, style: StyleOption)
    extends GeoRenderable with DataFrameLayer {
    private val dfc = df.collect() // expensive operation
    private val sft = SparkUtils.createFeatureType("sft", df.schema)
    // expensive map operation
    private val mappings = SparkUtils.rowsToFeatures(sft, df.schema).copy(id = r => r.getAs[String](idField))
    private val sftSeq = dfc.map(mappings.apply).toSeq
    private val sftLayer = SimpleFeatureLayerNonPoint(sftSeq, style)
    override def render: String = sftLayer.render
  }

  case class DataFrameLayerPoint(df: DataFrame, idField: String, style: StyleOption, radius: Double = 5.0)
    extends GeoRenderable with DataFrameLayer {
    private val dfc = df.collect() // expensive operation
    private val sft = SparkUtils.createFeatureType("sft", df.schema)
    // expensive map operation
    private val mappings = SparkUtils.rowsToFeatures(sft, df.schema).copy(id = r => r.getAs[String](idField))
    private val sftSeq = dfc.map(mappings.apply).toSeq
    private val sftLayer = SimpleFeatureLayerPoint(sftSeq, style, radius)
    override def render: String = sftLayer.render
  }

  case class SimpleFeatureLayerNonPoint(features: Seq[SimpleFeature], style: StyleOption)
    extends GeoRenderable with SimpleFeatureLayer {
    override def render: String =
      s"""{
         |L.geoJson(${features.map(simpleFeatureToGeoJSON).mkString("[",",","]")},
         |    {
         |      onEachFeature: onFeature,
         |      style: ${style.render}
         |    }
         |).addTo(map);
         |
         |${OnFeatureClick.render};}
       """.stripMargin
  }

  case class SimpleFeatureLayerPoint(features: Seq[SimpleFeature], style: StyleOption, radius: Double = 5.0)
    extends GeoRenderable with SimpleFeatureLayer {
    override def render: String =
      s"""{
          |L.geoJson(${features.map(simpleFeatureToGeoJSON).mkString("[",",","]")},
          |    {
          |      onEachFeature: onFeature,
          |      ${ PointToLayer(style, radius).render }
          |    }
          |).addTo(map);
          |
         |${OnFeatureClick.render};}
       """.stripMargin
  }

  case class Circle(cx: Double, cy: Double, radiusMeters: Double, style: StyleOption) extends GeoRenderable {
    override def render: String =
      s"""
         |L.circle([$cy, $cx], $radiusMeters, ${style.render}).addTo(map);
       """.stripMargin
  }

  implicit class JTSPolyLayer(style: StyleOption)(implicit val poly: Polygon) extends GeoRenderable {
    override def render: String = {
      val coords = poly.getCoordinates.map { c => Array(c.y, c.x).mkString("[", ",", "]") }.mkString("[", ",", "]")
      s"L.polygon($coords, ${style.render} ).addTo(map);"
    }
  }

  implicit class JTSPointLayer(style: StyleOption)(implicit val point: Point) extends GeoRenderable {
    override def render: String = s"L.circle([${point.getY},${point.getX}], 5, , { ${style.render} }).addTo(map);"
  }

  implicit class JTSLineStringLayer(style: StyleOption)(implicit val ls: LineString) extends GeoRenderable {
    override def render: String = {
      val coords = ls.getCoordinates.map { c => Array(c.y, c.x).mkString("[", ",", "]") }.mkString("[", ",", "]")
      s"L.polyline($coords, ${style.render} ).addTo(map);"
    }
  }

  val leafletCssCdn =
    """
      |<link rel="stylesheet" href="https://unpkg.com/leaflet@1.5.1/dist/leaflet.css"
      |  integrity="sha512-xwE/Az9zrjBIphAcBb3F6JVqxf46+CDLwfLMHloNu6KEQCAWi6HcDUbeOfBIptF7tcCzusKFjFw2yuvEpDL9wQ=="
      |  crossorigin=""/>
    """.stripMargin

  val leafletJsCdn =
    """
      |<script src="https://unpkg.com/leaflet@1.5.1/dist/leaflet.js"
      |  integrity="sha512-GffPMF3RvMeYyc1LWMHtK8EbPv0iNZ8/oTtHPx9/cc2ILxQ+u905qIwdpULaqDkyBKgOaB57QTMg7ztg8Jm2Og=="
      |  crossorigin=""></script>
    """.stripMargin

  val leafletWmsJsCdn =
    """
      |<script src="https://unpkg.com/leaflet.wms@0.2.0/dist/leaflet.wms.js"
      |  integrity="sha384-ixDRqhwaC8CiUqK6q3rR//gkxROH6Jo2ekssv8GY9ifDKsN53cvFhXof9rfb1Zhs"
      |  crossorigin=""></script>
    """.stripMargin

  def buildMap(layers: Seq[GeoRenderable], center: (Double, Double) = (0,0), zoom: Int = 8, path: Option[String] = None) = {

    val leafletCss = path match {
      case Some(p) => s"""<link rel="stylesheet" href="$p/leaflet.css" />"""
      case None => leafletCssCdn
    }

    val leafletJs = path match {
      case Some(p) => s"""<script src="$p/leaflet.js"></script>"""
      case None => leafletJsCdn
    }

    val leafletWmsJs = path match {
      case Some(p) => s"""<script src="$p/leaflet.wms.js"></script>"""
      case None => leafletWmsJsCdn
    }

    val countriesJs = path match {
      case Some(p) => s"""<script src="$p/countries.geo.js"></script>"""
      case None => "" // data not easily hosted anywhere
    }

    s"""
       |<html>
       |  <head>
       |    $leafletCss
       |    $leafletJs
       |    $leafletWmsJs
       |    $countriesJs
       |  </head>
       |  <body>
       |    <div id='map' style="width:100%;height:500px"></div>
       |    <script>
       |
       |      //'map' is the id of the map
       |      var map = L.map('map', {
       |        crs: L.CRS.EPSG4326,
       |        center: [${center._1}, ${center._2}],
       |        zoom: ${zoom}
       |      });
       |
       |      // Only add country boundaries if the geojson data loaded ok from the script
       |      if(typeof worldMap !== 'undefined') {
       |        // Initialize the Base Layer... Loaded from GeoJson
       |        var basestyle = {"color": "#717171", "weight": 2, "opacity": 1.0};
       |        var base = L.geoJson(worldMap, basestyle);
       |        map.addLayer(base);
       |      }
       |
       |      ${layers.map(_.render).mkString(sep = "\n")}
       |
       |    </script>
       |  </body>
       |</html>
     """.stripMargin
  }

  def render(layers: Seq[GeoRenderable], center: (Double, Double) = (0,0), zoom: Int = 8, path: Option[String] = None) = {
    val id = new RandomStringGenerator.Builder().withinRange('0', 'z').filteredBy(ASCII_ALPHA_NUMERALS).build().generate(5)
    s"""
       |<iframe id="${id}" sandbox="allow-scripts allow-same-origin" style="border:none;width:100%;height:520px" srcdoc="${xml.Utility.escape(buildMap(layers, center, zoom, path))}"></iframe>
       |<script>
       |  if(typeof resizeIFrame != 'function') {
       |    function resizeIFrame(el, k) {
       |      el.style.height = el.contentWindow.document.body.scrollHeight + 'px';
       |      el.style.width = '100%';
       |      if(k<=3) { setTimeout(function() { resizeIFrame(el, k+1)}, 1000) };
       |    }
       |  }
       |  $$().ready(function() {
       |    resizeIFrame($$('#$id').get(0), 1);
       |  });
       |</script>
     """.stripMargin
  }

  def show(layers: Seq[GeoRenderable], center: (Double, Double) = (0,0), zoom: Int = 1, path: Option[String] = None)(implicit disp: String => Unit) = disp(render(layers,center,zoom,path))

  def print(layers: Seq[GeoRenderable], center: (Double, Double) = (0,0), zoom: Int = 1, path: Option[String] = None) = println(buildMap(layers,center,zoom,path))
}
