/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.util.{Date, Locale, UUID}

import com.typesafe.config.{Config, ConfigFactory}
import com.vividsolutions.jts.geom._
import org.apache.commons.lang.StringEscapeUtils
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.SpecParser.{ListAttributeType, MapAttributeType, SimpleAttributeType}
import org.locationtech.geomesa.utils.stats.{Cardinality, IndexCoverage}
import org.locationtech.geomesa.utils.text.EnhancedTokenParsers
import org.opengis.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.{Failure, Success, Try}

object SimpleFeatureTypes {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors._

  val TABLE_SPLITTER           = "table.splitter.class"
  val TABLE_SPLITTER_OPTIONS   = "table.splitter.options"
  val ENABLED_INDEXES          = "geomesa.indexes.enabled"
  val MIXED_GEOMETRIES         = "geomesa.mixed.geometries"

  @deprecated
  val ENABLED_INDEXES_OLD      = "table.indexes.enabled"

  val OPT_DEFAULT              = "default"
  val OPT_SRID                 = "srid"
  val OPT_INDEX_VALUE          = "index-value"
  val OPT_INDEX                = "index"
  val OPT_CARDINALITY          = "cardinality"
  val OPT_BIN_TRACK_ID         = "bin-track-id"

  val OPTS = Seq(OPT_DEFAULT, OPT_SRID, OPT_INDEX, OPT_INDEX_VALUE, OPT_CARDINALITY, OPT_BIN_TRACK_ID)

  val USER_DATA_LIST_TYPE      = "subtype"
  val USER_DATA_MAP_KEY_TYPE   = "keyclass"
  val USER_DATA_MAP_VALUE_TYPE = "valueclass"

  /**
   * Create a SimpleFeatureType from a typesafe Config
   *
   * @param conf config
   * @param typeName optional typename to use for the SFT...will be overridden if the config contains a type-name key
   * @return
   */
  def createType(conf: Config, typeName: Option[String] = None, path: Option[String] = Some("sft")): SimpleFeatureType = {
    import org.locationtech.geomesa.utils.conf.ConfConversions._
    val toParse = path match {
      case Some(p) => conf.getConfigOpt(p).map(conf.withFallback).getOrElse(conf)
      case None    => conf
    }

    val nameSpec = toParse.getStringOpt("type-name").orElse(typeName).getOrElse(
      throw new IllegalArgumentException("Unable to parse type name from provided argument or config")
    )
    val (namespace, name) = buildTypeName(nameSpec)
    val specParser = new SpecParser

    val fields = getFieldConfig(toParse).map(buildField(_, specParser))
    val userData = if (toParse.hasPath("user-data")) {
      toParse.getConfig("user-data").entrySet().map(e => e.getKey -> e.getValue.unwrapped()).toMap
    } else {
      Map.empty[String, AnyRef]
    }
    val opts = userData.map { case (k, v) => new GenericOption(k, v) }.toSeq
    createType(namespace, name, fields, opts)
  }

  def getFieldConfig(conf: Config): Seq[Config] =
    if (conf.hasPath("fields")) { conf.getConfigList("fields") } else { conf.getConfigList("attributes") }

  def buildField(conf: Config, specParser: SpecParser): AttributeSpec = conf.getString("type") match {
    case t if simpleTypeMap.contains(t)   => SimpleAttributeSpec(conf)
    case t if geometryTypeMap.contains(t) => GeomAttributeSpec(conf)

    case t if specParser.parse(specParser.listType, t).successful => ListAttributeSpec(conf)
    case t if specParser.parse(specParser.mapType, t).successful  => MapAttributeSpec(conf)
  }

  def createType(nameSpec: String, spec: String): SimpleFeatureType = {
    val (namespace, name) = buildTypeName(nameSpec)
    val FeatureSpec(attributeSpecs, opts) = parse(spec)
    createType(namespace, name, attributeSpecs, opts)
  }

  def createType(namespace: String, name: String, spec: String): SimpleFeatureType = {
    createType(namespace + ":" + name, spec)
  }

  def buildTypeName(nameSpec: String): (String, String) = {
    val nsIndex = nameSpec.lastIndexOf(':')
    val (namespace, name) = if (nsIndex == -1 || nsIndex == nameSpec.length - 1) {
      (null, nameSpec)
    } else {
      (nameSpec.substring(0, nsIndex), nameSpec.substring(nsIndex + 1))
    }
    (namespace, name)
  }

  def createType(namespace: String,
                 name: String,
                 attributeSpecs: Seq[AttributeSpec],
                 opts: Seq[FeatureOption]): SimpleFeatureType = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val geomAttributes = attributeSpecs.collect { case g: GeomAttributeSpec => g }
    val dateAttributes = attributeSpecs.collect {
      case s: SimpleAttributeSpec if s.clazz == classOf[Date] => s
    }
    val defaultGeom = geomAttributes.find(_.options.get(OPT_DEFAULT).exists(_.toBoolean))
        .orElse(geomAttributes.headOption)
    // TODO GEOMESA-594 allow for setting default date field
    val defaultDate = dateAttributes.find(_.options.get(OPT_DEFAULT).exists(_.toBoolean))
        .orElse(dateAttributes.headOption)

    val b = new SimpleFeatureTypeBuilder()
    b.setNamespaceURI(namespace)
    b.setName(name)
    b.addAll(attributeSpecs.map(_.toAttribute))
    defaultGeom.foreach(dg => b.setDefaultGeometry(dg.name))
    val sft = b.buildFeatureType()
    defaultDate.foreach(dt => sft.setDtgField(dt.name))
    opts.foreach(_.decorateSFT(sft))
    sft
  }

  def renameSft(sft: SimpleFeatureType, newName: String) = {
    val builder = new SimpleFeatureTypeBuilder()
    builder.init(sft)
    builder.setName(newName)
    val renamed = builder.buildFeatureType()
    renamed.getUserData.putAll(sft.getUserData)
    renamed
  }

  def encodeType(sft: SimpleFeatureType, includeUserData: Boolean = false): String = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    lazy val userData = {
      val prefixes = sft.getUserDataPrefixes
      sft.getUserData.filter { case (k, v) => v != null && prefixes.exists(k.toString.startsWith) }
    }
    val suffix = if (!includeUserData || userData.isEmpty) { "" } else {
      userData.map { case (k, v) => s"$k='${StringEscapeUtils.escapeJava(v.toString)}'" }.mkString(";", ",", "")
    }

    sft.getAttributeDescriptors.map(encodeDescriptor(sft, _)).mkString("", ",", suffix)
  }


  def encodeDescriptor(sft: SimpleFeatureType, descriptor: AttributeDescriptor): String =
    AttributeSpecFactory.fromAttributeDescriptor(sft, descriptor).toSpec

  def getSecondaryIndexedAttributes(sft: SimpleFeatureType): Seq[AttributeDescriptor] =
    sft.getAttributeDescriptors.filter(ad => ad.isIndexed && !ad.isInstanceOf[GeometryDescriptor])

  object AttributeSpecFactory {
    def fromAttributeDescriptor(sft: SimpleFeatureType, ad: AttributeDescriptor) = {
      val options = scala.collection.mutable.Map.empty[String, String]
      ad.getIndexCoverage() match {
        case IndexCoverage.FULL => options.put(OPT_INDEX, IndexCoverage.FULL.toString)
        case IndexCoverage.JOIN => options.put(OPT_INDEX, IndexCoverage.JOIN.toString)
        case _ => // nothing
      }
      ad.getCardinality() match {
        case Cardinality.HIGH => options.put(OPT_CARDINALITY, Cardinality.HIGH.toString)
        case Cardinality.LOW  => options.put(OPT_CARDINALITY, Cardinality.LOW.toString)
        case _ => // nothing
      }
      if (ad.isIndexValue()) {
        options.put(OPT_INDEX_VALUE, "true")
      }
      if (ad.isBinTrackId) {
        options.put(OPT_BIN_TRACK_ID, "true")
      }
      ad.getType match {
        case t if simpleTypeMap.contains(t.getBinding.getSimpleName) =>
          SimpleAttributeSpec(ad.getLocalName, ad.getType.getBinding, options.toMap)

        case t if geometryTypeMap.contains(t.getBinding.getSimpleName) =>
          val srid = Option(ad.asInstanceOf[GeometryDescriptor].getCoordinateReferenceSystem)
              .flatMap(crs => Try(crs.getName.getCode.toInt).toOption)
              .getOrElse(4326)
          options.put(OPT_SRID, srid.toString)
          val default = sft.getGeometryDescriptor.equals(ad)
          GeomAttributeSpec(ad.getLocalName, ad.getType.getBinding, default, options.toMap)

        case t if t.getBinding.equals(classOf[java.util.List[_]]) =>
          ListAttributeSpec(ad.getLocalName, ad.getListType().get, options.toMap)

        case t if t.getBinding.equals(classOf[java.util.Map[_, _]]) =>
          val Some((keyType, valueType)) = ad.getMapTypes()
          MapAttributeSpec(ad.getLocalName, keyType, valueType, options.toMap)
      }
    }
  }

  sealed trait AttributeSpec {

    def name: String

    def clazz: Class[_]

    def options: Map[String, String]

    def getClassSpec: String = s"${typeEncode(clazz)}"

    def toSpec: String = {
      val builder = new StringBuilder(s"$name:$getClassSpec")
      for (opt <- OPTS; value <- options.get(opt)) {
        builder.append(s":$opt=$value")
      }
      builder.toString()
    }

    def toAttribute: AttributeDescriptor = {
      val builder = new AttributeTypeBuilder().binding(clazz)
      OPTS.foreach(opt => options.get(opt).foreach(value => builder.userData(opt, value)))
      addOptions(builder)
      builder.buildDescriptor(name)
    }

    protected def addOptions(builder: AttributeTypeBuilder): Unit = {}
  }

  implicit class AttributeCopyable(val attrSpec: AttributeSpec) extends AnyVal {
    def copy(): AttributeSpec = attrSpec match {
      case o: SimpleAttributeSpec => o.copy()
      case o: GeomAttributeSpec   => o.copy()
      case o: ListAttributeSpec   => o.copy()
      case o: MapAttributeSpec    => o.copy()
    }
  }

  object AttributeSpec {
    val defaults = Map[String, AnyRef](
      OPT_INDEX        -> IndexCoverage.NONE.toString,
      OPT_INDEX_VALUE  -> java.lang.Boolean.FALSE,
      OPT_CARDINALITY  -> Cardinality.UNKNOWN.toString,
      OPT_SRID         -> Integer.valueOf(4326),
      OPT_DEFAULT      -> java.lang.Boolean.FALSE,
      OPT_BIN_TRACK_ID -> java.lang.Boolean.FALSE
    )
    val fallback = ConfigFactory.parseMap(defaults)

    // back compatible with string or boolean
    def getIndexCoverage(conf: Config) =
      Try(conf.getString(OPT_INDEX)).flatMap(o => Try(IndexCoverage.withName(o.toLowerCase(Locale.US))))
        .orElse(Try(if (conf.getBoolean(OPT_INDEX)) IndexCoverage.JOIN else IndexCoverage.NONE))
        .getOrElse(IndexCoverage.NONE)

    def getOptions(conf: Config): Map[String, String] = {
      val options = scala.collection.mutable.Map.empty[String, String]
      AttributeSpec.getIndexCoverage(conf) match {
        case IndexCoverage.FULL => options.put(OPT_INDEX, IndexCoverage.FULL.toString)
        case IndexCoverage.JOIN => options.put(OPT_INDEX, IndexCoverage.JOIN.toString)
        case _ => // nothing
      }
      Cardinality.withName(conf.getString(OPT_CARDINALITY).toLowerCase(Locale.US)) match {
        case Cardinality.HIGH => options.put(OPT_CARDINALITY, Cardinality.HIGH.toString)
        case Cardinality.LOW  => options.put(OPT_CARDINALITY, Cardinality.LOW.toString)
        case _ => // nothing
      }
      if (conf.getBoolean(OPT_INDEX_VALUE)) {
        options.put(OPT_INDEX_VALUE, "true")
      }
      if (conf.getBoolean(OPT_BIN_TRACK_ID)) {
        options.put(OPT_BIN_TRACK_ID, "true")
      }
      //TODO GEOMESA-594 allow for setting default date field
      options.toMap
    }

    def standardizeOptions(options: Map[String, String], defaultGeom: Boolean = false): Map[String, String] = {
      val withIndex = if (defaultGeom) {
        options ++ Map(OPT_DEFAULT -> "true")
      } else {
        options.get(OPT_INDEX) match {
          case None => options
          case Some(index) =>
            Try(IndexCoverage.withName(index)) match {
              case Success(_) => options
              case Failure(_) if !java.lang.Boolean.valueOf(index) => options - OPT_INDEX
              case Failure(_) => options ++ Map(OPT_INDEX -> IndexCoverage.JOIN.toString)
            }
        }
      }
      withIndex
    }
  }

  sealed trait NonGeomAttributeSpec extends AttributeSpec

  object SimpleAttributeSpec {
    def apply(in: Config): SimpleAttributeSpec = {
      val conf = in.withFallback(AttributeSpec.fallback)
      val name     = conf.getString("name")
      val attrType = conf.getString("type")
      SimpleAttributeSpec(name, simpleTypeMap(attrType), AttributeSpec.getOptions(conf))
    }
  }

  case class SimpleAttributeSpec(name: String, clazz: Class[_], options: Map[String, String])
      extends NonGeomAttributeSpec

  object ListAttributeSpec {
    private val specParser = new SpecParser
    private val defaultType = ListAttributeType(SimpleAttributeType("string"))

    def apply(in: Config): ListAttributeSpec = {
      val conf          = in.withFallback(AttributeSpec.fallback)
      val name          = conf.getString("name")
      val attributeType = specParser.parse(specParser.listType, conf.getString("type")).getOrElse(defaultType)

      ListAttributeSpec(name, simpleTypeMap(attributeType.p.t), AttributeSpec.getOptions(conf))
    }
  }

  case class ListAttributeSpec(name: String, subClass: Class[_], options: Map[String, String])
      extends NonGeomAttributeSpec {

    val clazz = classOf[java.util.List[_]]

    override def getClassSpec = s"List[${typeEncode(subClass)}]"

    override def addOptions(builder: AttributeTypeBuilder) = {
      builder.collectionType(subClass)
    }
  }

  object MapAttributeSpec {
    private val specParser = new SpecParser
    private val defaultType = MapAttributeType(SimpleAttributeType("string"), SimpleAttributeType("string"))

    def apply(in: Config): MapAttributeSpec = {
      val conf          = in.withFallback(AttributeSpec.fallback)
      val name          = conf.getString("name")
      val attributeType = specParser.parse(specParser.mapType, conf.getString("type")).getOrElse(defaultType)

      MapAttributeSpec(name, simpleTypeMap(attributeType.kt.t), simpleTypeMap(attributeType.vt.t), Map.empty)
    }
  }

  case class MapAttributeSpec(name: String, keyClass: Class[_], valueClass: Class[_], options: Map[String, String])
      extends NonGeomAttributeSpec {

    val clazz = classOf[java.util.Map[_, _]]

    // TODO with lists too currently we only allow simple types in the ST IDX for simplicity - revisit if it becomes a use-case

    override def getClassSpec = s"Map[${typeEncode(keyClass)},${typeEncode(valueClass)}]"

    override def addOptions(builder: AttributeTypeBuilder) = {
      builder.mapTypes(keyClass, valueClass)
    }
  }

  object GeomAttributeSpec {
    def apply(in: Config): GeomAttributeSpec = {
      val conf = in.withFallback(AttributeSpec.fallback)

      val name     = conf.getString("name")
      val attrType = conf.getString("type")
      val default  = conf.getBoolean("default")

      val options = Map(
        OPT_SRID    -> conf.getInt("srid").toString,
        OPT_DEFAULT -> default.toString
      )

      GeomAttributeSpec(name, geometryTypeMap(attrType), default, options)
    }
  }

  case class GeomAttributeSpec(name: String, clazz: Class[_], default: Boolean, options: Map[String, String])
      extends AttributeSpec {

    override def toSpec = {
      val star = if (default) "*" else ""
      val builder = new StringBuilder(s"$star$name:$getClassSpec")
      for (opt <- OPTS; value <- options.get(opt)) {
        if (opt != OPT_DEFAULT) { // default geoms are indicated by the *
          builder.append(s":$opt=$value")
        }
      }
      builder.toString()
    }

    override def addOptions(builder: AttributeTypeBuilder) = {
      require(!options.get(OPT_SRID).exists(_.toInt != 4326),
        s"Invalid SRID '${options(OPT_SRID)}'. Only 4326 is supported.")
      builder.crs(CRS_EPSG_4326)
    }

  }

  sealed trait FeatureOption {
    def decorateSFT(sft: SimpleFeatureType): Unit
  }

  case class Splitter(splitterClazz: String, options: Map[String, String]) extends FeatureOption {
    override def decorateSFT(sft: SimpleFeatureType): Unit = {
      sft.getUserData.put(TABLE_SPLITTER, splitterClazz)
      sft.getUserData.put(TABLE_SPLITTER_OPTIONS, options)
    }
  }

  case class EnabledIndexes(indexes: List[String]) extends FeatureOption {
    override def decorateSFT(sft: SimpleFeatureType): Unit = {
      sft.getUserData.put(ENABLED_INDEXES, indexes.mkString(","))
    }
  }

  case class GenericOption(k: String, v: AnyRef) extends FeatureOption {
    override def decorateSFT(sft: SimpleFeatureType): Unit = {
      sft.getUserData.put(k, v)
    }
  }

  case class FeatureSpec(attributes: Seq[AttributeSpec], opts: Seq[FeatureOption])

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
    classOf[java.sql.Timestamp]  -> "Timestamp",
    classOf[java.util.List[_]]   -> "List",
    classOf[java.util.Map[_, _]] -> "Map",
    classOf[Array[Byte]]         -> "Bytes"
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
    "Date"              -> classOf[Date],
    "Timestamp"         -> classOf[Date],
    "byte[]"            -> classOf[Array[Byte]],
    "Bytes"             -> classOf[Array[Byte]]
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

  private val listTypeMap = Seq("list", "List", "java.util.List").map { n => (n, classOf[java.util.List[_]]) }.toMap
  private val mapTypeMap  = Seq("map", "Map", "java.util.Map").map { n => (n, classOf[java.util.Map[_, _]]) }.toMap

  object SpecParser {
    case class Name(s: String, default: Boolean = false)
    sealed trait AttributeType
    case class GeometryAttributeType(t: String) extends AttributeType
    case class SimpleAttributeType(t: String) extends AttributeType
    case class ListAttributeType(p: SimpleAttributeType) extends AttributeType
    case class MapAttributeType(kt: SimpleAttributeType, vt: SimpleAttributeType) extends AttributeType

    def optionToCardinality(options: Map[String, String]) = options.get(OPT_CARDINALITY)
        .flatMap(c => Try(Cardinality.withName(c.toLowerCase(Locale.US))).toOption)
        .getOrElse(Cardinality.UNKNOWN)

    def optionToIndexCoverage(options: Map[String, String]) = {
      val o = options.getOrElse(OPT_INDEX, IndexCoverage.NONE.toString)
      val fromName = Try(IndexCoverage.withName(o.toLowerCase(Locale.US)))
      fromName.getOrElse(if (Try(o.toBoolean).getOrElse(false)) IndexCoverage.JOIN else IndexCoverage.NONE)
    }
  }

   class KVPairParser(pairSep: String = ",", kvSep: String = ":") extends JavaTokenParsers {
    def k = "[0-9a-zA-Z\\.]+".r
    def v = s"[^($pairSep)^($kvSep)]+".r

    def kv = k ~ kvSep ~ v ^^ { case key ~ kvSep ~ value => key -> value }
    def kvList = repsep(kv, pairSep) ^^ { case x => x.toMap }

    def parse(s: String): Map[String, String] = {
      parse(kvList, s.trim) match {
        case Success(t, r)     if r.atEnd => t
        case NoSuccess(msg, r) if r.atEnd =>
          throw new IllegalArgumentException(s"Error parsing spec '$s' : $msg")
        case other =>
          throw new IllegalArgumentException(s"Error parsing spec '$s' : $other")
      }
    }
  }

  class ListSplitter(sep: String = ",") extends JavaTokenParsers {
    def v = s"[^($sep)]+".r
    def lst = repsep(v, sep)

    def parse(s: String): List[String] = {
      parse(lst, s.trim) match {
        case Success(t, r)     if r.atEnd => t
        case NoSuccess(msg, r) if r.atEnd =>
          throw new IllegalArgumentException(s"Error parsing spec '$s' : $msg")
        case other =>
          throw new IllegalArgumentException(s"Error parsing spec '$s' : $other")
      }
    }
  }
  private class SpecParser extends EnhancedTokenParsers {
    import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.SpecParser._
    /*
     Valid specs can have attributes that look like the following:
        "id:Integer:opt1=v1:opt2=v2,*geom:Geometry:srid=4326,ct:List[String]:index=true,mt:Map[String,Double]:index=false"
     */

    private val SEP = ":"

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
    def listTypeOuter: Parser[String]         = listTypeMap.keys.map(literal).reduce(_ | _)

    // valid maps
    def mapTypeOuter: Parser[String]          = mapTypeMap.keys.map(literal).reduce(_ | _)

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
    def option                = ("[a-zA-Z_.-]+".r <~ "=") ~ "[^:,;]+".r ^^ { case k ~ v => (k, v) }

    // builds a map of key/values
    def options               = repsep(option, SEP) ^^ { kvs => kvs.toMap }

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
        GeomAttributeSpec(n, geometryTypeMap(t), default, AttributeSpec.standardizeOptions(options, default))
    }

    // builds a NonGeomAttributeSpec for primitive types
    def simpleAttribute       = (name ~ SEP ~ simpleType ~ optionsOrEmptyMap) ^^ {
      case Name(n, default) ~ SEP ~ SimpleAttributeType(t) ~ options =>
        SimpleAttributeSpec(n, simpleTypeMap(t), AttributeSpec.standardizeOptions(options))
    }

    // builds a NonGeomAttributeSpec for complex types
    def complexAttribute      = (name ~ SEP ~ complexType ~ optionsOrEmptyMap) ^^ {
      case Name(n, default) ~ SEP ~ ListAttributeType(SimpleAttributeType(t)) ~ options =>
        ListAttributeSpec(n, simpleTypeMap(t), AttributeSpec.standardizeOptions(options))

      case Name(n, default) ~ SEP ~ MapAttributeType(SimpleAttributeType(kt), SimpleAttributeType(vt)) ~ options =>
        MapAttributeSpec(n, simpleTypeMap(kt), simpleTypeMap(vt), AttributeSpec.standardizeOptions(options))
    }

    // any attribute
    def attribute             = geometryAttribute | complexAttribute | simpleAttribute

    // Feature Option parsing
    private val EQ = "="

    def optValue = quotedString | nonQuotedString
    def fOptKey = "[a-zA-Z0-9\\.]+".r
    def fOptKeyValue =  (fOptKey <~ EQ) ~ optValue ^^ {  x => x._1 -> x._2 }

    def fOptList = repsep(fOptKeyValue, ",") ^^ { case optPairs =>
      val optMap = optPairs.toMap

      val splitterOpt = optMap.get(TABLE_SPLITTER).map { ts =>
        val tsOpts = optMap.get(TABLE_SPLITTER_OPTIONS).map(new KVPairParser().parse).getOrElse(Map.empty[String, String])
        Splitter(ts, tsOpts)
      }

      val enabledOpt = optMap.get(ENABLED_INDEXES).map(new ListSplitter().parse).map(EnabledIndexes)

      // other arbitrary options
      val known = Seq(TABLE_SPLITTER, TABLE_SPLITTER_OPTIONS, ENABLED_INDEXES)
      val others = optMap.filterKeys(k => !known.contains(k)).map{ case (k,v) => GenericOption(k, v) }

      (List(splitterOpt, enabledOpt).flatten ++ others).toSeq
    }

    // a full SFT spec
    def spec                  = repsep(attribute, ",") ~ (";" ~> fOptList).? ^^ {
      case attrs ~ fOpts => FeatureSpec(attrs, fOpts.getOrElse(Seq()))
    }

    def strip(s: String) = s.stripMargin('|').replaceAll("\\s*", "")

    def parse(s: String): FeatureSpec = parse(spec, strip(s)) match {
      case Success(t, r)     if r.atEnd => t
      case NoSuccess(msg, r) if r.atEnd =>
        throw new IllegalArgumentException(s"Error parsing spec '$s' : $msg")
      case other =>
        throw new IllegalArgumentException(s"Error parsing spec '$s' : $other")
    }
  }

  def parse(s: String): FeatureSpec = new SpecParser().parse(s)

}
