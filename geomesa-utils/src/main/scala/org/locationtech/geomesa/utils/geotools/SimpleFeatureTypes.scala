package org.locationtech.geomesa.utils.geotools

import java.util.{Date, UUID}

import com.vividsolutions.jts.geom._
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.utils.text.ObjectPoolFactory
import org.opengis.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.util.parsing.combinator.JavaTokenParsers

object SimpleFeatureTypes {

  def createType(nameSpec: String, spec: String): SimpleFeatureType = {
    val (namespace, name) =
      nameSpec.split(":").toList match {
        case n :: Nil => (null, n)
        case ns :: n :: Nil => (ns, n)
        case _ => throw new IllegalArgumentException(s"Invalid feature name: $nameSpec")
      }

    val attributeSpecs = parse(spec)

    val geomAttributes = attributeSpecs.collect { case g: GeomAttributeSpec => g }
    val defaultGeom = geomAttributes.find(_.default).orElse(geomAttributes.headOption)
    val b = new SimpleFeatureTypeBuilder()
    b.setNamespaceURI(namespace)
    b.setName(name)
    b.addAll(attributeSpecs.map(_.toAttribute))
    defaultGeom.foreach { dg => b.setDefaultGeometry(dg.name)}
    b.buildFeatureType()
  }

  def setCollectionType(ad: AttributeDescriptor, typ: Class[_]): Unit =
    ad.getUserData.put("subtype", typ)

  def getCollectionType(ad: AttributeDescriptor): Option[Class[_]] =
    Option(ad.getUserData.get("subtype")).map(_.asInstanceOf[Class[_]])

  def encodeType(sft: SimpleFeatureType): String =
    sft.getAttributeDescriptors.map { ad => AttributeSpecFactory.fromAttributeDescriptor(sft, ad).toSpec }.mkString(",")


  def getIndexedAttributes(sft: SimpleFeatureType): Seq[AttributeDescriptor] =
    sft.getAttributeDescriptors.filter(_.getUserData.getOrElse("index", false).asInstanceOf[java.lang.Boolean])

  object AttributeSpecFactory {
    def fromAttributeDescriptor(sft: SimpleFeatureType, ad: AttributeDescriptor) = ad.getType match {
      case t if simpleTypeMap.contains(t.getBinding.getSimpleName) =>
        SimpleAttributeSpec(ad.getLocalName, ad.getType.getBinding, ad.getUserData.getOrElse("index", false).asInstanceOf[Boolean])

      case t if geometryTypeMap.contains(t.getBinding.getSimpleName) =>
        GeomAttributeSpec(
          ad.getLocalName,
          ad.getType.getBinding,
          ad.getUserData.getOrElse("index", false).asInstanceOf[Boolean],
          if(ad.asInstanceOf[GeometryDescriptor].getCoordinateReferenceSystem.equals(DefaultGeographicCRS.WGS84)) 4326 else -1,
          sft.getGeometryDescriptor.equals(ad)
        )

      case t if t.getBinding.equals(classOf[java.util.List[_]]) =>
        ListAttributeSpec(ad.getLocalName, ad.getUserData.get("subtype").asInstanceOf[Class[_]], ad.getUserData.getOrElse("index", false).asInstanceOf[Boolean])

      case t if t.getBinding.equals(classOf[java.util.Map[_, _]]) =>
        MapAttributeSpec(ad.getLocalName,
          ad.getUserData.get("keyclass").asInstanceOf[Class[_]],
          ad.getUserData.get("valueclass").asInstanceOf[Class[_]],
          ad.getUserData.getOrElse("index", false).asInstanceOf[Boolean])
    }
  }

  trait AttributeSpec {
    def name: String

    def clazz: Class[_]

    def index: Boolean

    def toAttribute: AttributeDescriptor

    def toSpec: String
  }

  case class SimpleAttributeSpec(name: String, clazz: Class[_], index: Boolean) extends AttributeSpec {
    override def toAttribute: AttributeDescriptor = {
      val b = new AttributeTypeBuilder().binding(clazz).userData("index", index)
      b.buildDescriptor(name)
    }

    override def toSpec = s"$name:${typeEncode(clazz)}:index=$index"
  }

  case class ListAttributeSpec(name: String, subClass: Class[_], index: Boolean) extends AttributeSpec {
    val clazz = classOf[java.util.List[_]]

    override def toAttribute: AttributeDescriptor = {
      val b = new AttributeTypeBuilder().binding(clazz).userData("index", index)
      b.userData("subtype", subClass)
      b.buildDescriptor(name)
    }

    override def toSpec = s"$name:List[${subClass.getSimpleName}]:index=$index"
  }

  case class MapAttributeSpec(name: String, keyClass: Class[_], valueClass: Class[_], index: Boolean) extends AttributeSpec {
    val clazz = classOf[java.util.Map[_, _]]

    override def toAttribute: AttributeDescriptor = {
      val b = new AttributeTypeBuilder().binding(clazz).userData("index", index)
      b.userData("keyclass", keyClass)
      b.userData("valueclass", valueClass)
      b.buildDescriptor(name)
    }

    override def toSpec = s"$name:Map[${keyClass.getSimpleName},${valueClass.getSimpleName}]:index=$index"
  }

  case class GeomAttributeSpec(name: String, clazz: Class[_], index: Boolean, srid: Int, default: Boolean) extends AttributeSpec {
    override def toAttribute: AttributeDescriptor = {
      if (!(srid == 4326 || srid == -1)) {
        throw new IllegalArgumentException(s"Invalid SRID '$srid'. Only 4326 is supported.")
      }
      val b = new AttributeTypeBuilder()
      b.binding(clazz).userData("index", index).crs(DefaultGeographicCRS.WGS84).buildDescriptor(name)
    }

    override def toSpec = {
      val star = if (default) "*" else ""
      s"$star$name:${typeEncode(clazz)}:srid=$srid:index=$index"
    }
  }

  private val typeEncode: Map[Class[_], String] = Map(
    classOf[java.lang.String]    -> "String",
    classOf[java.lang.Integer]   -> "Integer",
    classOf[java.lang.Double]    -> "Double",
    classOf[java.lang.Long]      -> "Long",
    classOf[java.lang.Float]     -> "Float",
    classOf[java.lang.Boolean]   -> "Boolean",
    classOf[UUID]                -> "UUID",
    classOf[Geometry]            -> "Geometry",
    classOf[Point]               -> "Point",
    classOf[LineString]          -> "LineString",
    classOf[Polygon]             -> "Polygon",
    classOf[MultiPoint]          -> "MultiPoint",
    classOf[MultiLineString]     -> "MultiLineString",
    classOf[MultiPolygon]        -> "MultiPolygon",
    classOf[GeometryCollection]  -> "GeometryCollection",
    classOf[Date]                -> "Date",
    classOf[java.util.List[_]]   -> "List",
    classOf[java.util.Map[_, _]] -> "Map"
  )

  private val simpleTypeMap = Map(
    "String"            -> classOf[java.lang.String],
    "java.lang.String"  -> classOf[java.lang.String],
    "string"            -> classOf[java.lang.String],
    "Integer"           -> classOf[java.lang.Integer],
    "java.lang.Integer" -> classOf[java.lang.Integer],
    "int"               -> classOf[java.lang.Integer],
    "Int"               -> classOf[java.lang.Integer],
    "0"                 -> classOf[java.lang.Integer],
    "Long"              -> classOf[java.lang.Long],
    "java.lang.Long"    -> classOf[java.lang.Long],
    "long"              -> classOf[java.lang.Long],
    "Double"            -> classOf[java.lang.Double],
    "java.lang.Double"  -> classOf[java.lang.Double],
    "double"            -> classOf[java.lang.Double],
    "0.0"               -> classOf[java.lang.Double],
    "Float"             -> classOf[java.lang.Float],
    "java.lang.Float"   -> classOf[java.lang.Float],
    "float"             -> classOf[java.lang.Float],
    "0.0f"              -> classOf[java.lang.Float],
    "Boolean"           -> classOf[java.lang.Boolean],
    "java.lang.Boolean" -> classOf[java.lang.Boolean],
    "true"              -> classOf[java.lang.Boolean],
    "false"             -> classOf[java.lang.Boolean],
    "UUID"              -> classOf[UUID],
    "Date"              -> classOf[Date]
  )

  private val geometryTypeMap = Map(
    "Geometry"           -> classOf[Geometry],
    "Point"              -> classOf[Point],
    "LineString"         -> classOf[LineString],
    "Polygon"            -> classOf[Polygon],
    "MultiPoint"         -> classOf[MultiPoint],
    "MultiLineString"    -> classOf[MultiLineString],
    "MultiPolygon"       -> classOf[MultiPolygon],
    "GeometryCollection" -> classOf[GeometryCollection]
  )

  private class AttributeParser extends JavaTokenParsers {
    /*
     Valid specs can have attributes that look like the following:
        "id:Integer:opt1=v1,*geom:Geometry:srid=4326,ct:List[String]:index=true,mt:Map[String,Double]:index=false"
     */

    private val SEP = ":"

    case class Name(s: String, default: Boolean = false)
    sealed trait AttributeType
    case class GeometryAttributeType(t: String) extends AttributeType
    case class SimpleAttributeType(t: String) extends AttributeType
    case class ListAttributeType(p: SimpleAttributeType) extends AttributeType
    case class MapAttributeType(kt: SimpleAttributeType, vt: SimpleAttributeType) extends AttributeType

    def nonDefaultAttributeName  = """[^:,]+""".r ^^ { n => Name(n) }  // simple name
    def defaultAttributeName     = ("*" ~ nonDefaultAttributeName) ^^ {         // for *geom
      case "*" ~ Name(n, _)   => Name(n, default = true)
    }

    // matches any of the primitive types defined in simpleTypeMap
    // order matters so that Integer is matched before Int
    val simpleType =
      simpleTypeMap
        .keys
        .toList
        .sorted
        .reverse
        .map(literal)
        .reduce(_ | _) ^^ { case s => SimpleAttributeType(s) }

    // matches any of the geometry types defined in geometryTypeMap
    val geometryType =
      geometryTypeMap
        .keys
        .toList
        .sorted
        .reverse
        .map(literal)
        .reduce(_ | _) ^^ { case g => GeometryAttributeType(g) }

    // valid lists
    def listTypeOuter         = "List" | "list" | "java.util.List"

    // valid maps
    def mapTypeOuter          = "Map"  | "map"  | "java.util.Map"

    // list type matches "List[String]" or "List" (which defaults to parameterized type String)
    def listType              = listTypeOuter ~> ("[" ~> simpleType <~ "]").? ^^ {
      case st => ListAttributeType(st.getOrElse(SimpleAttributeType("String")))
    }

    // map type matches "Map[String,String]" (defaults to Map[String,String] if parameterized types not specified
    def mapType               = mapTypeOuter ~> ("[" ~> simpleType ~ "," ~ simpleType <~ "]").? ^^ {
      case Some(kt ~ "," ~ vt) => MapAttributeType(kt, vt)
      case None                => MapAttributeType(SimpleAttributeType("String"), SimpleAttributeType("String"))
    }

    // either a list or a map
    def complexType           = listType | mapType

    // simple type or geometry type or complex types
    def attrType              = simpleType | geometryType | complexType

    // converts options into key/values
    def option                = (ident <~ "=") ~ "[^:,]*".r ^^ { case k ~ v => (k, v) }

    // builds a map of key/values
    def options               = repsep(option, ":") ^^ { kvs => kvs.toMap }

    // options map or empty map if no options specified
    def optionsOrEmptyMap     = (SEP ~> options).? ^^ {
      case Some(opts) => opts
      case None       => Map.empty[String, String]
    }

    // either a name with default prefix or regular name
    def name                  = defaultAttributeName | nonDefaultAttributeName

    // builds a GeometrySpec
    def geometryAttribute     = (name ~ SEP ~ geometryType ~ optionsOrEmptyMap) ^^ {
      case Name(n, default) ~ SEP ~ GeometryAttributeType(t) ~ options =>
        val indexed = default || options.getOrElse("index", "false").toBoolean
        val srid    = options.getOrElse("srid", "4326").toInt
        GeomAttributeSpec(n, geometryTypeMap(t), indexed, srid, default)
    }

    // builds a NonGeomAttributeSpec for primitive types
    def simpleAttribute       = (name ~ SEP ~ simpleType ~ optionsOrEmptyMap) ^^ {
      case Name(n, default) ~ SEP ~ SimpleAttributeType(t) ~ options =>
        val indexed = options.getOrElse("index", "false").toBoolean
        SimpleAttributeSpec(n, simpleTypeMap(t), indexed)
    }

    // builds a NonGeomAttributeSpec for complex types
    def complexAttribute      = (name ~ SEP ~ complexType ~ optionsOrEmptyMap) ^^ {
      case Name(n, default) ~ SEP ~ ListAttributeType(SimpleAttributeType(t)) ~ options =>
        val indexed = options.getOrElse("index", "false").toBoolean
        ListAttributeSpec(n, simpleTypeMap(t), indexed)

      case Name(n, default) ~ SEP ~ MapAttributeType(SimpleAttributeType(kt), SimpleAttributeType(vt)) ~ options =>
        val indexed = options.getOrElse("index", "false").toBoolean
        MapAttributeSpec(n, simpleTypeMap(kt), simpleTypeMap(vt), indexed)
    }

    // any attribute
    def attribute             = geometryAttribute | complexAttribute | simpleAttribute

    // a full SFT spec
    def spec                  = repsep(attribute, ",") ^^ { attrs => attrs }

    def strip(s: String) = s.stripMargin('|').replaceAll("\\s*", "")

    def parse(s: String): Seq[AttributeSpec] = parse(spec, strip(s)) match {
      case Success(t, r)   if r.atEnd => t
      case Error(msg, r)   if r.atEnd => throw new IllegalArgumentException(msg)
      case Failure(msg, r) if r.atEnd => throw new IllegalArgumentException(msg)
      case _ => throw new IllegalArgumentException("Malformed attribute")
    }
  }

  private val parsers = ObjectPoolFactory(new AttributeParser, 10)
  def parse(s: String): Seq[AttributeSpec] = parsers.withResource { parser => parser.parse(s) }

}