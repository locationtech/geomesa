package org.locationtech.geomesa.utils.geotools

import java.lang
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom._
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.opengis.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.parsing.combinator.JavaTokenParsers

object SimpleFeatureTypes {

  def createType(nameSpec: String, spec: String): SimpleFeatureType = {
    val (namespace, name) =
      nameSpec.split(":").toList match {
        case n :: Nil       => (null, n)
        case ns :: n :: Nil => (ns, n)
        case _ => throw new IllegalArgumentException(s"Invalid feature name: $nameSpec")
      }
    val attributeSpecs = AttributeSpec.toAttributes(spec)
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
    val subBinding = Option(attr.getUserData.get("subtype"))
        .flatMap(o => Try(o.asInstanceOf[java.util.List[Class[_]]].map(typeEncode(_)).mkString("[", "][", "]")).toOption)
        .getOrElse("")
    val indexed = Try(attr.getUserData.get("index").asInstanceOf[lang.Boolean].booleanValue())
        .getOrElse(java.lang.Boolean.FALSE)
    s"$name:$binding$subBinding:index=$indexed"
  }

  private def encodeGeometryDescriptor(sft: SimpleFeatureType, gd: GeometryDescriptor): String = {
    val default = if (sft.getGeometryDescriptor.equals(gd)) "*" else ""
    val name = gd.getLocalName
    val binding = typeEncode(gd.getType.getBinding)
    val indexed = Try(gd.getUserData.get("index").asInstanceOf[lang.Boolean].booleanValue()).getOrElse(java.lang.Boolean.FALSE)
    s"$default$name:$binding:srid=4326:index=$indexed"
  }

  trait AttributeSpec {
    def name: String
    def clazz: Class[_]
    def index: Boolean
    def toAttribute: AttributeDescriptor
    def toSpec: String
  }

  case class NonGeomAttributeSpec(name: String, clazz: Class[_], subClass: Seq[Class[_]], index: Boolean) extends AttributeSpec {
    override def toAttribute: AttributeDescriptor = {
      val b = new AttributeTypeBuilder().binding(clazz).userData("index", index)
      if (!subClass.isEmpty) {
        b.userData("subtype", subClass.asJava)
      }
      b.buildDescriptor(name)
    }

    override def toSpec = s"$name:${typeEncode(clazz)}:index=$index"
  }

  case class GeomAttributeSpec(name: String, clazz: Class[_], index: Boolean, srid: Int, default: Boolean) extends AttributeSpec {
    override def toAttribute: AttributeDescriptor = {
      if (srid != 4326) {
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

  object AttributeSpec {

    private def parseKV(s: String): (String, String) =
      s.split("=") match { case Array(k, v) => k -> v }

    def buildGeomSpec(name: String, clazz: Class[_], opts: Map[String, String]) = {
      val defaultGeom = name.startsWith("*")
      val indexed = defaultGeom || Try(opts("index").toBoolean).getOrElse(false)
      val attrName = if(defaultGeom) name.drop(1) else name
      val srid = Try(opts("srid").toInt).getOrElse(4326)
      GeomAttributeSpec(attrName, clazz, indexed, srid, default = defaultGeom)
    }

    def buildNonGeomSpec(name: String, clazz: Class[_], subClass: Seq[Class[_]], opts: Map[String, String]) = {
      val indexed = Try(opts("index").toBoolean).getOrElse(false)
      NonGeomAttributeSpec(name, clazz, subClass, indexed)
    }

    def apply(spec: String): AttributeSpec = {
      val name :: classString :: xs = spec.split(":").toList
      val clas = parseClass(classString)
      val opts = xs.map(parseKV).toMap
      if (classOf[Geometry].isAssignableFrom(clas.main)) {
        buildGeomSpec(name, clas.main, opts)
      } else {
        buildNonGeomSpec(name, clas.main, clas.subtypes, opts)
      }
    }

    /**
     * Converts a string from a spec into a concrete class, plus any type classes.
     *
     * @param classString
     * @return
     */
    def parseClass(classString: String): ParsedClass =
      if (simpleTypeMap.contains(classString)) {
        ParsedClass(simpleTypeMap(classString))
      } else {
        val parseResult = ParseClassStrings.parseAll(ParseClassStrings.classDef, classString)
        if (!parseResult.successful) {
          throw new IllegalArgumentException(s"Invalid attribute type: $classString")
        }
        val classDef = parseResult.get
        val complexType = complexTypeMap.get(classDef.main)
            .getOrElse(throw new IllegalArgumentException(s"Invalid attribute type: ${classDef.main}"))
        val subClasses = if (classDef.subtypes.isEmpty) {
          (1 to complexType.getTypeParameters.size).map(_ => classOf[String])
        } else {
          classDef.subtypes.map(parseClass).map(_.main).toSeq
        }
        if (subClasses.size != complexType.getTypeParameters.size) {
          throw new IllegalArgumentException(s"Invalid number of type parameters for type ${classDef.main}. " +
              s"Expected ${complexType.getTypeParameters.size}, found ${subClasses.size}")
        }
        ParsedClass(complexType, subClasses)
      }

    def toAttributes(spec: String): Array[AttributeSpec] = spec.split(",").map(AttributeSpec(_))

    def toString(spec: Seq[AttributeSpec]) = spec.map(_.toSpec).mkString(",")
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
    "String"              -> classOf[java.lang.String],
    "java.lang.String"    -> classOf[java.lang.String],
    "string"              -> classOf[java.lang.String],
    "Integer"             -> classOf[java.lang.Integer],
    "java.lang.Integer"   -> classOf[java.lang.Integer],
    "int"                 -> classOf[java.lang.Integer],
    "Int"                 -> classOf[java.lang.Integer],
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

  private val complexTypeMap = Map(
    "List"            -> classOf[java.util.List[_]],
    "java.util.List"  -> classOf[java.util.List[_]],
    "list"            -> classOf[java.util.List[_]],
    "Map"             -> classOf[java.util.Map[_, _]],
    "java.util.Map"   -> classOf[java.util.Map[_, _]],
    "map"             -> classOf[java.util.Map[_, _]]
  )
}

/**
 * Parses class strings from simple feature type specifications. Will match strings of the form:
 *
 * String
 * java.lang.String
 * java.util.List[String]
 * Map[String][Int]
 *
 */
object ParseClassStrings extends JavaTokenParsers {

  def classDef: Parser[ParsedClassString] = classIdentifier~rep(parameterizedType) ^^
      { case main~sub => ParsedClassString(main, sub) }

  def classIdentifier: Parser[String] = "(\\w|\\.)+".r
  def parameterizedType: Parser[String] = "["~>classIdentifier<~"]"
}

case class ParsedClassString(main: String, subtypes: Seq[String] = Seq.empty)

case class ParsedClass(main: Class[_], subtypes: Seq[Class[_]] = Seq.empty)
