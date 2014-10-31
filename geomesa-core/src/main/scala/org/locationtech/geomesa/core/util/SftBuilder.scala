/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.core.util

import java.util.{Date, UUID}

import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.core.util.SftBuilder._
import org.locationtech.geomesa.data.TableSplitter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Splitter

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe.{Type => UType, _}


class SftBuilder {

  private val entries = new ListBuffer[String]
  private var splitterOpt: Option[Splitter] = None
  private var dtgFieldOpt: Option[String] = None

  // Primitives
  def stringType(name: String, index: Boolean = false)  = append(name, index, "String")
  def intType(name: String, index: Boolean = false)     = append(name, index, "Integer")
  def longType(name: String, index: Boolean = false)    = append(name, index, "Long")
  def floatType(name: String, index: Boolean = false)   = append(name, index, "Float")
  def doubleType(name: String, index: Boolean = false)  = append(name, index, "Double")
  def booleanType(name: String, index: Boolean = false) = append(name, index, "Boolean")

  // Helpful Types
  def date(name: String, index: Boolean = false, default: Boolean = false) = {
    if (default) {
      withDefaultDtg(name)
    }
    append(name, index, "Date")
  }
  def uuid(name: String, index: Boolean = false) = append(name, index, "UUID")

  // Single Geometries
  def point(name: String, default: Boolean = false, index: Boolean = false) =
    appendGeom(name, index, default, "Point")
  def lineString(name: String, default: Boolean = false, index: Boolean = false) =
    appendGeom(name, index, default, "LineString")
  def polygon(name: String, default: Boolean = false, index: Boolean = false) =
    appendGeom(name, index, default, "Polygon")
  def geometry(name: String, default: Boolean = false, index: Boolean = false) =
    appendGeom(name, index, default, "Geometry")

  // Multi Geometries
  def multiPoint(name: String, default: Boolean = false, index: Boolean = false) =
    appendGeom(name, index, default, "MultiPoint")
  def multiLineString(name: String, default: Boolean = false, index: Boolean = false) =
    appendGeom(name, index, default, "MultiLineString")
  def multiPolygon(name: String, default: Boolean = false, index: Boolean = false) =
    appendGeom(name, index, default, "MultiPolygon")
  def geometryCollection(name: String, default: Boolean = false, index: Boolean = false) =
    appendGeom(name, index, default, "GeometryCollection")

  // List and Map Types
  def mapType[K: TypeTag, V: TypeTag](name: String, index: Boolean = false) =
    append(name, index, s"Map[${resolve(typeOf[K])},${resolve(typeOf[V])}]")
  def listType[T: TypeTag](name: String, index: Boolean = false) = append(name, index, s"List[${resolve(typeOf[T])}]")

  def recordSplitter(clazz: String, splitOptions: Map[String,String]) = {
    this.splitterOpt = Some(Splitter(clazz, splitOptions))
    this
  }

  def recordSplitter(clazz: Class[_ <: TableSplitter], splitOptions: Map[String,String]): SftBuilder = {
    recordSplitter(clazz.getName, splitOptions)
    this
  }

  def withDefaultDtg(field: String): SftBuilder = {
    dtgFieldOpt = Some(field)
    this
  }

  def defaultDtg() = withDefaultDtg("dtg")

  // Internal helper methods
  private def resolve(tt: UType): String =
    tt match {
      case t if primitiveTypes.contains(tt) => simpleClassName(tt.toString)
      case t if tt == typeOf[Date]          => "Date"
      case t if tt == typeOf[UUID]          => "UUID"
    }

  private def append(name: String, index: Boolean, typeStr: String) = {
    val parts = List(name, typeStr) ++ indexPart(index)
    entries += parts.mkString(SepPart)
    this
  }

  private def appendGeom(name: String, index: Boolean, default: Boolean, typeStr: String) = {
    val namePart  = if (default) "*" + name else name
    val parts     = List(namePart, typeStr, SridPart) ++ indexPart(index || default) //force index on default geom
    entries += parts.mkString(SepPart)
    this
  }

  private def indexPart(index: Boolean) = if (index) Some("index=true") else None

  // note that SimpleFeatureTypes requires that splitter and splitter opts be ordered properly
  private def splitPart = splitterOpt.map { s =>
    List(
      SimpleFeatureTypes.TABLE_SPLITTER + "=" + s.splitterClazz,
      SimpleFeatureTypes.TABLE_SPLITTER_OPTIONS + "=" + encodeMap(s.options, SepPart, SepEntry)
    ).mkString(",")
  }

  // public accessors
  /** Get the type spec string associated with this builder...doesn't include dtg info */
  def getSpec() = {
    val entryLst = List(entries.mkString(SepEntry))
    val splitLst = splitPart.map(List(_)).getOrElse(List())
    (entryLst ++ splitLst).mkString(";")
  }

  /** builds a SimpleFeatureType object from this builder */
  def build(nameSpec: String) = {
    val sft = SimpleFeatureTypes.createType(nameSpec, this.getSpec)
    dtgFieldOpt.map(sft.getUserData.put(SF_PROPERTY_START_TIME, _))
    sft
  }

}

object SftBuilder {

  // Note: not for general use - only for use with SimpleFeatureTypes parsing (doesn't escape separator characters)
  def encodeMap(opts: Map[String,String], kvSep: String, entrySep: String) =
    opts.map { case (k, v) => (k + kvSep + v) }.mkString(entrySep)

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

  def simpleClassName(clazz: String) = clazz.split("[.]").last

}
