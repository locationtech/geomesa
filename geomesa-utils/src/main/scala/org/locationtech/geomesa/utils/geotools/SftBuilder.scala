/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.util.{Date, UUID}

import org.locationtech.geomesa.utils.geotools.SftBuilder._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs._
import org.locationtech.geomesa.utils.stats.Cardinality
import org.locationtech.geomesa.utils.stats.Cardinality.Cardinality
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe.{Type => UType, _}

@deprecated("SchemaBuilder")
abstract class InitBuilder[T] {
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  private val entries = new ListBuffer[String]
  private val options = new ListBuffer[String]
  private var dtgFieldOpt: Option[String] = None

  // Primitives - back compatible
  def stringType(name: String, index: Boolean): T =
    stringType(name, Opts(index = index))
  def stringType(name: String, index: Boolean, stIndex: Boolean): T =
    stringType(name, Opts(index = index, stIndex = stIndex))
  def intType(name: String, index: Boolean): T =
    intType(name, Opts(index = index))
  def intType(name: String, index: Boolean, stIndex: Boolean): T =
    intType(name, Opts(index = index, stIndex = stIndex))
  def longType(name: String, index: Boolean): T =
    longType(name, Opts(index = index))
  def longType(name: String, index: Boolean, stIndex: Boolean): T =
    longType(name, Opts(index = index, stIndex = stIndex))
  def floatType(name: String, index: Boolean): T =
    floatType(name, Opts(index = index))
  def floatType(name: String, index: Boolean, stIndex: Boolean): T =
    floatType(name, Opts(index = index, stIndex = stIndex))
  def doubleType(name: String, index: Boolean): T =
    doubleType(name, Opts(index = index))
  def doubleType(name: String, index: Boolean, stIndex: Boolean): T =
    doubleType(name, Opts(index = index, stIndex = stIndex))
  def booleanType(name: String, index: Boolean): T =
    booleanType(name, Opts(index = index))
  def booleanType(name: String, index: Boolean, stIndex: Boolean): T =
    booleanType(name, Opts(index = index, stIndex = stIndex))

  // Primitives
  def stringType (name: String, opts: Opts = Opts()): T = append(name, opts, "String")
  def intType    (name: String, opts: Opts = Opts()): T = append(name, opts, "Integer")
  def longType   (name: String, opts: Opts = Opts()): T = append(name, opts, "Long")
  def floatType  (name: String, opts: Opts = Opts()): T = append(name, opts, "Float")
  def doubleType (name: String, opts: Opts = Opts()): T = append(name, opts, "Double")
  def booleanType(name: String, opts: Opts = Opts()): T = append(name, opts, "Boolean")

  // Helpful Types - back compatible
  def date(name: String, default: Boolean): T =
    date(name, Opts(default = default))
  def date(name: String, index: Boolean, default: Boolean): T =
    date(name, Opts(index = index, default = default))
  def date(name: String, index: Boolean, stIndex: Boolean, default: Boolean): T =
    date(name, Opts(index = index, stIndex = stIndex, default = default))
  def uuid(name: String, index: Boolean): T =
    uuid(name, Opts(index = index))
  def uuid(name: String, index: Boolean, stIndex: Boolean): T =
    uuid(name, Opts(index = index, stIndex = stIndex))

  // Helpful Types
  def date(name: String, opts: Opts = Opts()): T = {
    if (opts.default) {
      withDefaultDtg(name)
    }
    append(name, opts, "Date")
  }
  def uuid(name: String, opts: Opts = Opts()): T = append(name, opts, "UUID")
  def bytes(name: String, opts: Opts = Opts()): T = append(name, opts, "Bytes")

  // Single Geometries
  def point     (name: String, default: Boolean = false): T = appendGeom(name, default, "Point")
  def lineString(name: String, default: Boolean = false): T = appendGeom(name, default, "LineString")
  def polygon   (name: String, default: Boolean = false): T = appendGeom(name, default, "Polygon")
  def geometry  (name: String, default: Boolean = false): T = appendGeom(name, default, "Geometry")

  // Multi Geometries
  def multiPoint     (name: String, default: Boolean = false): T = appendGeom(name, default, "MultiPoint")
  def multiLineString(name: String, default: Boolean = false): T = appendGeom(name, default, "MultiLineString")
  def multiPolygon   (name: String, default: Boolean = false): T = appendGeom(name, default, "MultiPolygon")
  def geometryCollection(name: String, default: Boolean = false): T =
    appendGeom(name, default, "GeometryCollection")

  // List and Map Types - back compatible
  def mapType[K: TypeTag, V: TypeTag](name: String, index: Boolean): T =
    mapType[K, V](name, Opts(index = index))
  def listType[Type: TypeTag](name: String, index: Boolean): T =
    listType[Type](name, Opts(index = index))

  // List and Map Types
  def mapType[K: TypeTag, V: TypeTag](name: String, opts: Opts = Opts()): T =
    append(name, opts.copy(stIndex = false), s"Map[${resolve(typeOf[K])},${resolve(typeOf[V])}]")
  def listType[Type: TypeTag](name: String, opts: Opts = Opts()): T =
    append(name, opts.copy(stIndex = false), s"List[${resolve(typeOf[Type])}]")

  // Convenience method to add columns via Attribute Descriptors
  def attributeDescriptor(ad: AttributeDescriptor): T =
    append(ad.getLocalName, Opts(), ad.getType.getBinding.getCanonicalName)

  def withIndexes(indexSuffixes: List[String]): T = userData(ENABLED_INDICES, indexSuffixes.mkString(","))

  def userData(key: String, value: String): T = {
    options.append(s"$key='$value'")
    this.asInstanceOf[T]
  }

  def withDefaultDtg(field: String): T = {
    dtgFieldOpt = Some(field)
    this.asInstanceOf[T]
  }

  def defaultDtg(): T = withDefaultDtg("dtg")

  // Internal helper methods
  private def resolve(tt: UType): String =
    tt match {
      case t if primitiveTypes.contains(tt) => simpleClassName(tt.toString)
      case t if tt == typeOf[Date]          => "Date"
      case t if tt == typeOf[UUID]          => "UUID"
      case t if tt == typeOf[Array[Byte]]   => "Bytes"
    }

  private def append(name: String, opts: Opts, typeStr: String) = {
    val parts = List(name, typeStr) ++ indexPart(opts.index) ++ stIndexPart(opts.stIndex) ++
        cardinalityPart(opts.cardinality)
    entries += parts.mkString(SepPart)
    this.asInstanceOf[T]
  }

  private def appendGeom(name: String, default: Boolean, typeStr: String) = {
    val namePart = if (default) "*" + name else name
    val parts = List(namePart, typeStr, SridPart) ++
        indexPart(default) ++ //force index on default geom
        stIndexPart(default)
    entries += parts.mkString(SepPart)
    this.asInstanceOf[T]
  }

  private def indexPart(index: Boolean) = if (index) Seq(s"$OPT_INDEX=true") else Seq.empty
  private def stIndexPart(index: Boolean) = if (index) Seq(s"$OPT_INDEX_VALUE=true") else Seq.empty
  private def cardinalityPart(cardinality: Cardinality) = cardinality match {
    case Cardinality.LOW | Cardinality.HIGH => Seq(s"$OPT_CARDINALITY=${cardinality.toString}")
    case _ => Seq.empty
  }

  // public accessors
  /** Get the type spec string associated with this builder...doesn't include dtg info */
  def getSpec: String = {
    if (options.isEmpty) {
      entries.mkString(SepEntry)
    } else {
      s"${entries.mkString(SepEntry)};${options.mkString(SepEntry)}"
    }
  }

  /** builds a SimpleFeatureType object from this builder */
  def build(nameSpec: String): SimpleFeatureType = {
    val sft = SimpleFeatureTypes.createType(nameSpec, getSpec)
    dtgFieldOpt.foreach(sft.setDtgField)
    sft
  }

}

@deprecated("SchemaBuilder")
class SftBuilder extends InitBuilder[SftBuilder] {}

object SftBuilder {

  case class Opts(index: Boolean = false,
                  stIndex: Boolean = false,
                  default: Boolean = false,
                  cardinality: Cardinality = Cardinality.UNKNOWN)

  // Note: not for general use - only for use with SimpleFeatureTypes parsing (doesn't escape separator characters)
  def encodeMap(opts: Map[String,String], kvSep: String, entrySep: String): String =
    opts.map { case (k, v) => k + kvSep + v }.mkString(entrySep)

  val SridPart = "srid=4326"
  val SepPart  = ":"
  val SepEntry = ","

  val primitiveTypes =
    List(
      typeOf[java.lang.String],
      typeOf[String],
      typeOf[java.lang.Integer],
      typeOf[Int],
      typeOf[java.lang.Long],
      typeOf[Long],
      typeOf[java.lang.Double],
      typeOf[Double],
      typeOf[java.lang.Float],
      typeOf[Float],
      typeOf[java.lang.Boolean],
      typeOf[Boolean]
    )

  def simpleClassName(clazz: String): String = clazz.split("[.]").last

}
