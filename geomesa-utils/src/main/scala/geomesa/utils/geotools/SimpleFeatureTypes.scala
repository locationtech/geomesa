package geomesa.utils.geotools

import java.lang
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom._
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.opengis.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.util.Try

object SimpleFeatureTypes {

  def createType(nameSpec: String, spec: String): SimpleFeatureType = {
    val (namespace, name) =
      nameSpec.split(":").toList match {
        case n :: Nil       => (null, n)
        case ns :: n :: Nil => (ns, n)
        case _ => throw new IllegalArgumentException(s"Invalid feature name: $nameSpec")
      }
    val attributeSpecs = spec.split(",").map(AttributeSpec(_))
    val geomAttributes = attributeSpecs.collect { case g: GeomAttributeSpec => g }
    val defaultGeom = geomAttributes.find(_.default).orElse(geomAttributes.headOption)
    val b = new SimpleFeatureTypeBuilder()
    b.setNamespaceURI(namespace)
    b.setName(name)
    b.addAll(attributeSpecs.map(_.toAttribute))
    defaultGeom.foreach { dg => b.setDefaultGeometry(dg.name) }
    b.buildFeatureType()
  }

  def encodeType(sft: SimpleFeatureType): String =
    sft.getAttributeDescriptors.map {
      case gd: GeometryDescriptor => encodeGeometryDescriptor(sft, gd)
      case attr                   => encodeNonGeomDescriptor(attr)
    }.mkString(",")

  def getIndexedAttributes(sft: SimpleFeatureType): Seq[AttributeDescriptor] =
      sft.getAttributeDescriptors.filter(_.getUserData.getOrElse("index", false).asInstanceOf[java.lang.Boolean])

  private def encodeNonGeomDescriptor(attr: AttributeDescriptor): String = {
    val name = attr.getLocalName
    val binding = typeEncode(attr.getType.getBinding)
    val indexed = Try(attr.getUserData.get("index").asInstanceOf[lang.Boolean].booleanValue()).getOrElse(java.lang.Boolean.FALSE)
    s"$name:$binding:index=$indexed"
  }

  private def encodeGeometryDescriptor(sft: SimpleFeatureType, gd: GeometryDescriptor): String = {
    val default = if (sft.getGeometryDescriptor.equals(gd)) "*" else ""
    val name = gd.getLocalName
    val binding = typeEncode(gd.getType.getBinding)
    val indexed = Try(gd.getUserData.get("index").asInstanceOf[lang.Boolean].booleanValue()).getOrElse(java.lang.Boolean.FALSE)
    val epsg = Try(CRS.lookupEpsgCode(gd.getCoordinateReferenceSystem, true)).getOrElse(Integer.valueOf(-1))
    s"$default$name:$binding:srid=$epsg:index=$indexed"
  }

  private trait AttributeSpec {
    def name: String
    def clazz: Class[_]
    def index: Boolean
    def toAttribute: AttributeDescriptor
  }

  private case class NonGeomAttributeSpec(name: String, clazz: Class[_], index: Boolean) extends AttributeSpec {
    override def toAttribute: AttributeDescriptor = {
      val b = new AttributeTypeBuilder()
      b.binding(clazz).userData("index", index).buildDescriptor(name)
    }
  }

  private case class GeomAttributeSpec(name: String, clazz: Class[_], index: Boolean, srid: Int, default: Boolean) extends AttributeSpec {
    override def toAttribute: AttributeDescriptor = {
      val b = new AttributeTypeBuilder()
      val crs = Try(CRS.decode(s"EPSG:$srid")).getOrElse(DefaultGeographicCRS.WGS84)
      b.binding(clazz).userData("index", index).crs(crs).buildDescriptor(name)
    }
  }

  private object AttributeSpec {
    private def parseKV(s: String): (String, String) =
      s.split("=") match { case Array(k, v) => k -> v }

    def buildGeomSpec(name: String, clazz: Class[_], opts: Map[String, String]) = {
      val indexed = Try(opts("index").toBoolean).getOrElse(false)
      val defaultGeom = name.startsWith("*")
      val attrName = if(defaultGeom) name.drop(1) else name
      val srid = Try(opts("srid").toInt).getOrElse(4326)
      GeomAttributeSpec(attrName, clazz, indexed, srid, default = defaultGeom)
    }

    def buildNonGeomSpec(name: String, clazz: Class[_], opts: Map[String, String]) = {
      val indexed = Try(opts("index").toBoolean).getOrElse(false)
      NonGeomAttributeSpec(name, clazz, indexed)
    }

    def apply(spec: String): AttributeSpec = {
      val name :: clazz :: xs = spec.split(":").toList
      val opts = xs.map(parseKV).toMap
      typeMap(clazz) match {
        case c if classOf[Geometry].isAssignableFrom(c) => buildGeomSpec(name, c, opts)
        case c => buildNonGeomSpec(name, c, opts)
      }
    }
  }

  private val typeEncode: Map[Class[_], String] = Map(
    classOf[java.lang.String]   -> "String",
    classOf[java.lang.Integer]  -> "Integer",
    classOf[java.lang.Double]   -> "Double",
    classOf[java.lang.Long]     -> "Long",
    classOf[java.lang.Float]    -> "Float",
    classOf[java.lang.Boolean]  -> "Boolean",
    classOf[UUID]               -> "UUID",
    classOf[Geometry]           -> "Geometry",
    classOf[Point]              -> "Point",
    classOf[LineString]         -> "LineString",
    classOf[Polygon]            -> "Polygon",
    classOf[MultiPoint]         -> "MultiPoint",
    classOf[MultiLineString]    -> "MultiLineString",
    classOf[MultiPolygon]       -> "MultiPolygon",
    classOf[GeometryCollection] -> "GeometryCollection",
    classOf[Date]               -> "Date"
  )
  
  private val typeMap = Map(
    "String"              -> classOf[java.lang.String],
    "java.lang.String"    -> classOf[java.lang.String],
    "string"              -> classOf[java.lang.String],
    "Integer"             -> classOf[java.lang.Integer],
    "java.lang.Integer"   -> classOf[java.lang.Integer],
    "int"                 -> classOf[java.lang.Integer],
    "0"                   -> classOf[java.lang.Integer],
    "Long"                -> classOf[java.lang.Long],
    "java.lang.Long"      -> classOf[java.lang.Long],
    "long"                -> classOf[java.lang.Long],
    "Double"              -> classOf[java.lang.Double],
    "java.lang.Double"    -> classOf[java.lang.Double],
    "double"              -> classOf[java.lang.Double],
    "0.0"                 -> classOf[java.lang.Double],
    "Float"               -> classOf[java.lang.Float],
    "java.lang.Float"     -> classOf[java.lang.Float],
    "float"               -> classOf[java.lang.Float],
    "0.0f"                -> classOf[java.lang.Float],
    "Boolean"             -> classOf[java.lang.Boolean],
    "java.lang.Boolean"   -> classOf[java.lang.Boolean],
    "true"                -> classOf[java.lang.Boolean],
    "false"               -> classOf[java.lang.Boolean],
    "UUID"                -> classOf[UUID],
    "Geometry"            -> classOf[Geometry],
    "Point"               -> classOf[Point],
    "LineString"          -> classOf[LineString],
    "Polygon"             -> classOf[Polygon],
    "MultiPoint"          -> classOf[MultiPoint],
    "MultiLineString"     -> classOf[MultiLineString],
    "MultiPolygon"        -> classOf[MultiPolygon],
    "GeometryCollection"  -> classOf[GeometryCollection],
    "Date"                -> classOf[Date]
  )
}
