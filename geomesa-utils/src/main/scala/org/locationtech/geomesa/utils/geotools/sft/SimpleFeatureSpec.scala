/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools.sft

import java.util.regex.Pattern
import java.util.{Date, UUID}

import org.apache.commons.text.StringEscapeUtils
import org.geotools.feature.AttributeTypeBuilder
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpec.AttributeSpec
import org.locationtech.jts.geom._
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Intermediate format for simple feature types. Used for converting between string specs,
  * typesafe config specs and `SimpleFeatureType`s
  *
  * @param attributes attributes
  * @param options simple feature level optinos
  */
case class SimpleFeatureSpec(attributes: Seq[AttributeSpec], options: Map[String, AnyRef])

object SimpleFeatureSpec {

  /**
    * Intermediate format for attribute descriptors
    */
  sealed trait AttributeSpec {

    /**
      * Attribute name
      *
      * @return name
      */
    def name: String

    /**
      * Type binding
      *
      * @return class binding
      */
    def clazz: Class[_]

    /**
      * Attribute level options - all options are stored as strings for simplicity.
      * @see `RichAttributeDescriptors` for conversions.
      *
      * @return attribute level options
      */
    def options: Map[String, String]

    /**
      * Convert to a spec string
      *
      * @return a partial spec string
      */
    def toSpec: String = {
      val opts = specOptions.map { case (k, v) =>
        if (simpleOptionPattern.matcher(v).matches()) {
          s":$k=$v"
        } else {
          s":$k='${StringEscapeUtils.escapeJava(v)}'"
        }
      }
      s"$name:$getClassSpec${opts.mkString}"
    }

    /**
      * Convert to a typesafe config map
      *
      * @return a spec map
      */
    def toConfigMap: Map[String, String] = Map("name" -> name, "type" -> getClassSpec) ++ configOptions

    /**
      * Convert to an attribute descriptor
      *
      * @return a descriptor
      */
    def toDescriptor: AttributeDescriptor = {
      val builder = new AttributeTypeBuilder().binding(clazz)
      descriptorOptions.foreach { case (k, v) => builder.userData(k, v) }
      builderHook(builder)
      builder.buildDescriptor(name)
    }

    /**
      * Gets class binding as a spec string
      *
      * @return class part of spec string
      */
    protected def getClassSpec: String = typeEncode(clazz)

    /**
      * Options encoded in the spec string
      *
      * @return options to include in the spec string conversion
      */
    protected def specOptions: Map[String, String] = options

    /**
      * Options encoded in the config map
      *
      * @return options to include in the config map conversion
      */
    protected def configOptions: Map[String, String] = options

    /**
      * Options set in the attribute descriptor
      *
      * @return options to include in the descriptor conversion
      */
    protected def descriptorOptions: Map[String, String] = options

    /**
      * Hook for modifying attribute descriptor
      *
      * @param builder attribute desctiptor builder
      */
    protected def builderHook(builder: AttributeTypeBuilder): Unit = {}
  }

  import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions.{OPT_DEFAULT, OPT_INDEX, OPT_SRID}

  private val simpleOptionPattern = Pattern.compile("[a-zA-Z0-9_]+")

  def attribute(sft: SimpleFeatureType, descriptor: AttributeDescriptor): AttributeSpec = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    import scala.collection.JavaConversions._

    val name = descriptor.getLocalName
    val binding = descriptor.getType.getBinding
    val options = descriptor.getUserData.map { case (k, v) => k.toString -> v.toString }.toMap

    if (simpleTypeMap.contains(binding.getSimpleName)) {
      SimpleAttributeSpec(name, binding, options)
    } else if (geometryTypeMap.contains(binding.getSimpleName)) {
      val opts = if (sft != null && sft.getGeometryDescriptor == descriptor) { options + (OPT_DEFAULT -> "true") } else { options }
      GeomAttributeSpec(name, binding, opts)
    } else if (classOf[java.util.List[_]].isAssignableFrom(binding)) {
      val itemClass = Option(descriptor.getListType()).getOrElse(classOf[String])
      ListAttributeSpec(name, itemClass, options)
    } else if (classOf[java.util.Map[_, _]].isAssignableFrom(binding)) {
      val (keyBinding, valueBinding) = descriptor.getMapTypes()
      val keyClass = Option(keyBinding).getOrElse(classOf[String])
      val valueClass = Option(valueBinding).getOrElse(classOf[String])
      MapAttributeSpec(name, keyClass, valueClass, options)
    } else {
      throw new IllegalArgumentException(s"Unknown type binding $binding")
    }
  }

  /**
    * Simple attribute
    */
  case class SimpleAttributeSpec(name: String, clazz: Class[_], options: Map[String, String]) extends AttributeSpec

  /**
    * Geometry attribute
    */
  case class GeomAttributeSpec(name: String, clazz: Class[_], options: Map[String, String]) extends AttributeSpec {

    private val default = options.get(OPT_DEFAULT).exists(_.toBoolean)

    override def toSpec: String = if (default) { s"*${super.toSpec}" } else { super.toSpec }

    override def builderHook(builder: AttributeTypeBuilder): Unit = {
      require(!options.get(OPT_SRID).exists(_.toInt != 4326),
        s"Invalid SRID '${options(OPT_SRID)}'. Only 4326 is supported.")
      builder.crs(org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)
    }

    // default geoms are indicated by the *
    // we don't allow attribute indexing for geometries
    override protected def specOptions: Map[String, String] = options - OPT_DEFAULT - OPT_INDEX
    override protected def configOptions: Map[String, String] = options - OPT_INDEX
    override protected def descriptorOptions: Map[String, String] = options - OPT_INDEX
  }

  /**
    * List attribute
    */
  case class ListAttributeSpec(name: String, subClass: Class[_], options: Map[String, String]) extends AttributeSpec {
    import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeConfigs.USER_DATA_LIST_TYPE

    override val clazz: Class[java.util.List[_]] = classOf[java.util.List[_]]
    override val getClassSpec = s"List[${typeEncode(subClass)}]"

    override protected def specOptions: Map[String, String] = options - USER_DATA_LIST_TYPE
    override protected def descriptorOptions: Map[String, String] = options + (USER_DATA_LIST_TYPE -> subClass.getName)
  }

  /**
    * Map attribute
    */
  case class MapAttributeSpec(name: String, keyClass: Class[_], valueClass: Class[_], options: Map[String, String])
      extends AttributeSpec {
    import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeConfigs._

    override val clazz: Class[java.util.Map[_, _]] = classOf[java.util.Map[_, _]]
    override val getClassSpec = s"Map[${typeEncode(keyClass)},${typeEncode(valueClass)}]"

    override protected def specOptions: Map[String, String] =
      options - USER_DATA_MAP_VALUE_TYPE - USER_DATA_MAP_KEY_TYPE
    override protected def descriptorOptions: Map[String, String] =
      options + (USER_DATA_MAP_KEY_TYPE -> keyClass.getName) + (USER_DATA_MAP_VALUE_TYPE -> valueClass.getName)
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
    classOf[java.sql.Date]       -> "Date",
    classOf[java.sql.Timestamp]  -> "Timestamp",
    classOf[java.util.List[_]]   -> "List",
    classOf[java.util.Map[_, _]] -> "Map",
    classOf[Array[Byte]]         -> "Bytes"
  )

  private [sft] val simpleTypeMap = Map(
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
    "boolean"           -> classOf[java.lang.Boolean],
    "bool"              -> classOf[java.lang.Boolean],
    "java.lang.Boolean" -> classOf[java.lang.Boolean],
    "true"              -> classOf[java.lang.Boolean],
    "false"             -> classOf[java.lang.Boolean],
    "UUID"              -> classOf[UUID],
    "Date"              -> classOf[Date],
    "Timestamp"         -> classOf[java.sql.Timestamp],
    "byte[]"            -> classOf[Array[Byte]],
    "Bytes"             -> classOf[Array[Byte]]
  )

  private [sft] val geometryTypeMap = Map(
    "Geometry"           -> classOf[Geometry],
    "Point"              -> classOf[Point],
    "LineString"         -> classOf[LineString],
    "Polygon"            -> classOf[Polygon],
    "MultiPoint"         -> classOf[MultiPoint],
    "MultiLineString"    -> classOf[MultiLineString],
    "MultiPolygon"       -> classOf[MultiPolygon],
    "GeometryCollection" -> classOf[GeometryCollection]
  )

  private [sft] val listTypeMap = Map(
    "list"           -> classOf[java.util.List[_]],
    "List"           -> classOf[java.util.List[_]],
    "java.util.List" -> classOf[java.util.List[_]])

  private [sft] val mapTypeMap  = Map(
    "map"           -> classOf[java.util.Map[_, _]],
    "Map"           -> classOf[java.util.Map[_, _]],
    "java.util.Map" -> classOf[java.util.Map[_, _]])
}
