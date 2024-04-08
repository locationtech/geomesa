/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import com.google.gson._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom._
import org.locationtech.jts.geom.impl.CoordinateArraySequence

import java.util.Collections


trait GeoJsonParsing {

  import GeoJsonParsing._

  import scala.collection.JavaConverters._

  private val factory = new GeometryFactory

  /**
    * Determines if the element is a geojson feature collection
    *
    * @param el element
    * @return
    */
  def isFeatureCollection(el: JsonElement): Boolean = isType(el, FeatureCollectionType)

  /**
    * Determines if the element is a geojson feature
    *
    * @param el element
    * @return
    */
  def isFeature(el: JsonElement): Boolean = isType(el, FeatureType)

  /**
    * Parse a geojson feature collection element
    *
    * @see `isFeatureCollection` to determine if this is likely to succeed
    *
    * @param el element
    * @return
    */
  def parseFeatureCollection(el: JsonElement): Seq[GeoJsonFeature] = {
    val features = el.getAsJsonObject.get(FeaturesKey).getAsJsonArray
    Seq.tabulate(features.size)(i => parseFeature(features.get(i)))
  }

  /**
    * Parse a geojson feature element
    *
    * @see `isFeature` to determine if this is likely to succeed
    *
    * @param el element
    * @return
    */
  def parseFeature(el: JsonElement): GeoJsonFeature = {
    val obj = el.getAsJsonObject
    val id = obj.get(IdKey) match {
      case s: JsonPrimitive if s.isString => Some(s.getAsString)
      case _ => None
    }
    val geometry = parseGeometry(obj.get(GeometryKey))
    val props: Map[String, Any] = obj.get(PropertiesKey) match {
      case o: JsonObject => parseElement(o, s"$$['$PropertiesKey']").toMap
      case _ => Map.empty
    }
    GeoJsonFeature(id, geometry, props)
  }

  /**
    * Parse a geometry element
    *
    * @param el element
    * @return
    */
  def parseGeometry(el: JsonElement): Geometry = el match {
    case o: JsonObject    => parseGeometryObject(o)
    case o: JsonPrimitive => WKTUtils.read(o.getAsString)
    case _: JsonNull      => null.asInstanceOf[Geometry]
    case _ => throw new IllegalArgumentException(s"Unknown geometry type: $el")
  }

  /**
    * Parse a geometry object
    *
    * @param obj object
    * @return
    */
  private def parseGeometryObject(obj: JsonObject): Geometry = {
    obj.get(TypeKey).getAsString.toLowerCase match {
      case "point" =>
        factory.createPoint(toPointCoords(obj.get(CoordinatesKey)))

      case "linestring" =>
        factory.createLineString(toCoordSeq(obj.get(CoordinatesKey)))

      case "polygon" =>
        toPolygon(obj.get(CoordinatesKey))

      case "multipoint" =>
        factory.createMultiPoint(toCoordSeq(obj.get(CoordinatesKey)))

      case "multilinestring" =>
        val coords = obj.get(CoordinatesKey).getAsJsonArray.asScala
            .map(c => factory.createLineString(toCoordSeq(c))).toArray
        factory.createMultiLineString(coords)

      case "multipolygon" =>
        factory.createMultiPolygon(obj.get(CoordinatesKey).getAsJsonArray.asScala.map(toPolygon).toArray)

      case "geometrycollection" =>
        factory.createGeometryCollection(obj.get(GeometriesKey).getAsJsonArray.asScala.map(parseGeometry).toArray)

      case unknown =>
        throw new NotImplementedError(s"Can't parse geometry type of $unknown")
    }
  }

  private def toPointCoords(el: JsonElement): Coordinate = {
    el.getAsJsonArray.asScala.map(_.getAsDouble).toSeq match {
      case Seq(x, y)    => new Coordinate(x, y)
      case Seq(x, y, z) => new Coordinate(x, y, z)
      case s => throw new IllegalArgumentException(s"Invalid point - expected 2 or 3 values, got ${s.mkString(", ")}")
    }
  }

  private def toCoordSeq(el: JsonElement): CoordinateSequence =
    new CoordinateArraySequence(el.getAsJsonArray.asScala.map(_.getAsJsonArray).map(toPointCoords).toArray)

  private def toPolygon(el: JsonElement): Polygon = {
    val rings = el.getAsJsonArray.iterator.asScala.map(c => factory.createLinearRing(toCoordSeq(c)))
    val shell = rings.next
    if (rings.hasNext) {
      factory.createPolygon(shell, rings.toArray)
    } else {
      factory.createPolygon(shell)
    }
  }

  private def isType(el: JsonElement, t: String): Boolean = el match {
    case o: JsonObject => Option(o.get(TypeKey)).exists(e => e.isJsonPrimitive && e.getAsString == t)
    case _ => false
  }
}

object GeoJsonParsing {

  import scala.collection.JavaConverters._

  /**
   * Parsed geojson feature element
   *
   * @param id id, if present
   * @param geom geometry
   * @param properties 'properties' values - key is json path to value, value is a string, boolean or list[string].
   *                   nested elements will be flattened out, with a path pointing into the element
   */
  case class GeoJsonFeature(id: Option[String], geom: Geometry, properties: Map[String, Any])

  private val FeatureType = "Feature"
  private val FeatureCollectionType = "FeatureCollection"

  private val TypeKey = "type"
  private val FeaturesKey = "features"
  private val CoordinatesKey = "coordinates"
  private val PropertiesKey = "properties"
  private val GeometryKey = "geometry"
  private val GeometriesKey = "geometries"
  private val IdKey = "id"

  /**
   * Parse a json element
   *
   * @param elem element
   * @param path json path to the object
   * @return map of key is json path to value, value is a string, boolean or list[string]
   */
  def parseElement(elem: JsonElement, path: String): Seq[(String, Any)] = {
    elem match {
      case e: JsonPrimitive =>
        if (e.isBoolean) {
          Seq(path -> e.getAsBoolean)
        } else {
          // note: gson numbers don't have a defined type so type checking doesn't work
          Seq(path -> e.getAsString)
        }

      case e: JsonObject =>
        val builder = Seq.newBuilder[(String, Any)]
        e.entrySet().asScala.foreach { entry =>
          builder ++= parseElement(entry.getValue, s"$path['${entry.getKey}']")
        }
        builder.result

      case e: JsonArray =>
        val list = new java.util.ArrayList[String](e.size())
        var i = 0
        while (i < e.size()) {
          list.add(e.get(i).toString)
          i += 1
        }
        Seq(path -> Collections.unmodifiableList(list))

      case _ =>
        Seq.empty // no-op
    }
  }
}
