/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import com.google.gson.JsonElement
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import org.locationtech.geomesa.utils.text.WKTUtils


trait GeoJsonParsing {

  import scala.collection.JavaConverters._

  private val CoordsPath = "coordinates"

  private val geoFac = new GeometryFactory

  def toPointCoords(el: JsonElement): Coordinate = {
    val Seq(x, y) = el.getAsJsonArray.iterator.asScala.map(_.getAsDouble).toSeq
    new Coordinate(x, y)
  }

  def toCoordSeq(el: JsonElement): CoordinateSequence =
    new CoordinateArraySequence(el.getAsJsonArray.iterator.asScala.map(_.getAsJsonArray).map(toPointCoords).toArray)

  def toPolygon(el: JsonElement): Polygon = {
    val rings = el.getAsJsonArray.iterator.asScala.map(c => geoFac.createLinearRing(toCoordSeq(c)))
    val shell = rings.next
    if (rings.hasNext) {
      geoFac.createPolygon(shell, rings.toArray)
    } else {
      geoFac.createPolygon(shell)
    }
  }

  def parseGeometry(el: JsonElement): Geometry = {
    if (el.isJsonObject) {
      val geomType = el.getAsJsonObject.get("type").getAsString.toLowerCase
      geomType match {
        case "point" =>
          geoFac.createPoint(toPointCoords(el.getAsJsonObject.get(CoordsPath)))
        case "linestring" =>
          geoFac.createLineString(toCoordSeq(el.getAsJsonObject.get(CoordsPath)))
        case "polygon" =>
          toPolygon(el.getAsJsonObject.get(CoordsPath))
        case "multipoint" =>
          geoFac.createMultiPoint(toCoordSeq(el.getAsJsonObject.get(CoordsPath)))
        case "multilinestring" =>
          val coords = el.getAsJsonObject.get(CoordsPath).getAsJsonArray
              .iterator.asScala.map(c => geoFac.createLineString(toCoordSeq(c))).toArray
          geoFac.createMultiLineString(coords)
        case "multipolygon" =>
          val polys = el.getAsJsonObject.get(CoordsPath).getAsJsonArray.iterator.asScala.map(toPolygon).toArray
          geoFac.createMultiPolygon(polys)
      }
    } else if (el.isJsonNull) {
      null.asInstanceOf[Geometry]
    } else if (el.isJsonPrimitive) {
      WKTUtils.read(el.getAsString)
    } else {
      throw new IllegalArgumentException(s"Unknown geometry type: $el")
    }
  }
}
