/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.geojson.query

import java.io.ByteArrayOutputStream

import com.vividsolutions.jts.geom.Geometry
import org.geotools.geojson.geom.GeometryJSON
import org.json4s.{JArray, JObject, JValue}
import org.locationtech.geomesa.features.kryo.json.JsonPathParser
import org.locationtech.geomesa.features.kryo.json.JsonPathParser.{PathAttribute, PathElement}
import org.opengis.filter.Filter
import org.opengis.filter.expression.PropertyName

import scala.util.control.NonFatal

/**
  * Query trait
  */
sealed trait GeoJsonQuery {
  def toFilter(idPath: Option[Seq[PathElement]], dtgPath: Option[Seq[PathElement]]): Filter
}

/**
  * Query constructs for geojson features. Syntax is based on mongodb queries.
  *
  * Available predicates:
  *   Include
  *     {} - return all features
  *   Equality
  *     { "foo" : "bar" } - find all features with an attribute named foo equal to bar
  *   Less than, greater than
  *     { "foo" : { "$lt" : 10 } }
  *     { "foo" : { "$gt" : 10 } }
  *     { "foo" : { "$lte" : 10 } }
  *     { "foo" : { "$gte" : 10 } }
  *   Spatial
  *     { geometry : { "$bbox" : [-180, -90, 180, 90] } }
  *     { geometry : { "$intersects" : { "$geometry" : { "type" : "Point", "coordinates" : [30, 10] } } } }
  *     { geometry : { "$within" : { "$geometry" : { "type" : "Polygon", "coordinates": [ [ [0,0], [3,6], [6,1], [0,0] ] ] } } } }
  *     { geometry : { "$contains" : { "$geometry" : { "type" : "Point", "coordinates" : [30, 10] } } } }
  *   And/Or
  *     { "foo" : "bar", "baz" : 10 }
  *     { "$or" : [ { "foo" : "bar" }, { "baz" : 10 } ] }
  */
object GeoJsonQuery {

  import org.locationtech.geomesa.filter.ff

  private [query] val GeoJsonGeometryPath = Seq(PathAttribute("geometry"))

  private val jsonGeometry = new GeometryJSON()

  /**
    * Parse a query string
    *
    * @param query query string
    * @return
    */
  def apply(query: String): GeoJsonQuery = {
    import org.json4s._
    import org.json4s.native.JsonMethods._

    if (query == null || query.isEmpty) { Include } else {
      try {
        parse(query) match {
          case j: JObject => evaluate(j)
          case _ => throw new IllegalArgumentException("Invalid input - expected JSON object")
        }
      } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid query string:\n$query", e)
      }
    }
  }

  /**
    * Create a query from a parsed json object
    *
    * @param json query json
    * @return
    */
  private def evaluate(json: JObject): GeoJsonQuery = {
    if (json.obj.isEmpty) { Include } else {
      val predicates = json.obj.map {
        case (prop, v: JObject) =>
          evaluatePredicate(JsonPathParser.parse(prop), v)

        case ("$or", v: JArray) =>
          val ors = v.arr.map {
            case o: JObject => evaluate(o)
            case o => throw new IllegalArgumentException(s"Expected Object, got $o")
          }
          Or(ors: _*)

        case (prop, v: JValue) =>
          Equals(JsonPathParser.parse(prop), v.values)
      }
      if (predicates.length > 1) { And(predicates: _*) } else { predicates.head }
    }
  }

  /**
    * Evaluate a complex predicate
    *
    * @param path path that the predicate operates on
    * @param json predicate json object
    * @return
    */
  private def evaluatePredicate(path: Seq[PathElement], json: JObject): GeoJsonQuery = {
    json.obj.headOption match {
      case Some(("$bbox", v: JArray)) =>
        // { "$bbox" : [-180, -90, 180, 90] }
        val List(xmin, ymin, xmax, ymax) = v.values.asInstanceOf[List[Number]]
        Bbox(path, xmin.doubleValue, ymin.doubleValue, xmax.doubleValue, ymax.doubleValue)

      case Some(("$intersects", v: JObject)) =>
        // { "$intersects" : { "$geometry" : { "type" : "Point", "coordinates" : [30, 10] } } }
        Intersects(path, evaluateGeometry(v))

      case Some(("$within", v: JObject)) =>
        // { "$within" : { "$geometry" : { "type" : "Polygon", "coordinates": [ [ [0,0], [3,6], [6,1], [0,0] ] ] } } }
        Within(path, evaluateGeometry(v))

      case Some(("$contains", v: JObject)) =>
        // { "$contains" : { "$geometry" : { "type" : "Point", "coordinates" : [30, 10] } } }
        Contains(path, evaluateGeometry(v))

      case Some(("$lt", v)) =>
        // { "$lt" : 10 }
        LessThan(path, v.values, inclusive = false)

      case Some(("$lte", v)) =>
        // { "$lte" : 10 }
        LessThan(path, v.values, inclusive = true)

      case Some(("$gt", v)) =>
        // { "$gt" : 10 }
        GreaterThan(path, v.values, inclusive = false)

      case Some(("$gte", v)) =>
        // { "$gte" : 10 }
        GreaterThan(path, v.values, inclusive = true)

      case Some((p, v)) =>
        throw new IllegalArgumentException(s"Invalid predicate '$p'")

      case None =>
        throw new IllegalArgumentException("Invalid json structure")
    }
  }

  /**
    * Read a json geometry object:
    * { "$geometry" : { "type" : "Point", "coordinates" : [30, 10] } }
    *
    * @param json geometry object
    * @return
    */
  private def evaluateGeometry(json: JObject): Geometry = {
    import org.json4s.native.JsonMethods._
    val geom = json.obj.find(_._1 == "$geometry").map(_._2).getOrElse {
      throw new IllegalArgumentException(s"Expected $$geometry, got ${json.obj.map(_._1).mkString(", ")}")
    }
    jsonGeometry.read(compact(render(geom)))
  }

  /**
    * Converts a json-path into a filter attribute for geojson simple features. Simple feature type
    * is assumed to be one defined by @see GeoJsonGtIndex.spec
    *
    * @param path json-path
    * @param dtgPath path of date field in the sft, if any
    * @return
    */
  private def filterAttribute(path: Seq[PathElement], dtgPath: Option[Seq[PathElement]]): PropertyName = {
    if (path == GeoJsonGeometryPath) {
      ff.property("geom")
    } else if (dtgPath.exists(_ == path)) {
      ff.property("dtg")
    } else {
      ff.property(JsonPathParser.print(PathAttribute("json") +: path))
    }
  }

  /**
    * Format a geometry as required for a json query predicate
    *
    * @param geometry geometry
    * @return
    */
  private def printJson(geometry: Geometry): String = {
    val bytes = new ByteArrayOutputStream
    jsonGeometry.write(geometry, bytes)
    s"""{"$$geometry":${bytes.toString}}"""
  }

  /**
    * Format a value as required for json - e.g. quote when necessary
    *
    * @param value value
    * @return
    */
  private def printJson(value: Any): String = {
    value match {
      case null => "null"
      case s: String => s""""$s""""
      case v => v.toString
    }
  }

  /**
    * All features
    */
  case object Include extends GeoJsonQuery {
    override def toFilter(idPath: Option[Seq[PathElement]], dtgPath: Option[Seq[PathElement]]): Filter = Filter.INCLUDE
    override val toString = "{}"
  }

  /**
    * Spatial intersect
    *
    * @param path property to evaluate
    * @param geometry geometry to compare with property value
    */
  case class Intersects(path: Seq[PathElement], geometry: Geometry) extends GeoJsonQuery {
    override def toFilter(idPath: Option[Seq[PathElement]], dtgPath: Option[Seq[PathElement]]): Filter =
      ff.intersects(filterAttribute(path, None), ff.literal(geometry))
    override def toString =
      s"""{"${JsonPathParser.print(path, dollar = false)}":{"$$intersects":${printJson(geometry)}}}"""
  }

  object Intersects {
    def apply(geometry: Geometry): Intersects = Intersects(GeoJsonGeometryPath, geometry)
    def apply(path: String, geometry: Geometry): Intersects = Intersects(JsonPathParser.parse(path), geometry)
  }

  /**
    * Spatial within
    *
    * @param path property to evaluate
    * @param geometry geometry to compare with property value
    */
  case class Within(path: Seq[PathElement], geometry: Geometry) extends GeoJsonQuery {
    override def toFilter(idPath: Option[Seq[PathElement]], dtgPath: Option[Seq[PathElement]]): Filter =
      ff.within(filterAttribute(path, None), ff.literal(geometry))
    override def toString =
      s"""{"${JsonPathParser.print(path, dollar = false)}":{"$$within":${printJson(geometry)}}}"""
  }

  object Within {
    def apply(geometry: Geometry): Within = Within(GeoJsonGeometryPath, geometry)
    def apply(path: String, geometry: Geometry): Within = Within(JsonPathParser.parse(path), geometry)
  }

  /**
    * Spatial contains
    *
    * @param path property to evaluate
    * @param geometry geometry to compare with property value
    */
  case class Contains(path: Seq[PathElement], geometry: Geometry) extends GeoJsonQuery {
    override def toFilter(idPath: Option[Seq[PathElement]], dtgPath: Option[Seq[PathElement]]): Filter =
      ff.contains(filterAttribute(path, None), ff.literal(geometry))
    override def toString =
      s"""{"${JsonPathParser.print(path, dollar = false)}":{"$$contains":${printJson(geometry)}}}"""
  }

  object Contains {
    def apply(geometry: Geometry): Contains = Contains(GeoJsonGeometryPath, geometry)
    def apply(path: String, geometry: Geometry): Contains = Contains(JsonPathParser.parse(path), geometry)
  }

  /**
    * Spatial bounding box
    *
    * @param path property to evaluate
    * @param xmin min x value
    * @param ymin min y value
    * @param xmax max x value
    * @param ymax max y value
    */
  case class Bbox(path: Seq[PathElement], xmin: Double, ymin: Double, xmax: Double, ymax: Double) extends GeoJsonQuery {
    override def toFilter(idPath: Option[Seq[PathElement]], dtgPath: Option[Seq[PathElement]]): Filter =
      ff.bbox(filterAttribute(path, None), xmin, ymin, xmax, ymax, "4326")
    override def toString = s"""{"${JsonPathParser.print(path, dollar = false)}":{"$$bbox":[$xmin,$ymin,$xmax,$ymax]}}"""
  }

  object Bbox {
    def apply(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Bbox =
      Bbox(GeoJsonGeometryPath, xmin, ymin, xmax, ymax)
    def apply(path: String, xmin: Double, ymin: Double, xmax: Double, ymax: Double): Bbox =
      Bbox(JsonPathParser.parse(path), xmin, ymin, xmax, ymax)
  }

  /**
    * Equality
    *
    * @param path property to evaluate
    * @param value value to compare with property value
    */
  case class Equals(path: Seq[PathElement], value: Any) extends GeoJsonQuery {
    override def toFilter(idPath: Option[Seq[PathElement]], dtgPath: Option[Seq[PathElement]]): Filter = {
      if (idPath.exists(_ == path)) {
        val fids = value match {
          case v: Iterable[_] => v.map(_.toString).toSeq
          case v => Seq(v.toString)
        }
        ff.id(fids.map(ff.featureId): _*)
      } else {
        ff.equals(filterAttribute(path, dtgPath), ff.literal(value))
      }
    }
    override def toString = s"""{"${JsonPathParser.print(path, dollar = false)}":${printJson(value)}}"""
  }

  object Equals {
    def apply(path: String, value: Any): Equals = Equals(JsonPathParser.parse(path), value)
  }

  /**
    * Less than comparison
    *
    * @param path property to evaluate
    * @param value value to compare with property value
    * @param inclusive inclusive bounds
    */
  case class LessThan(path: Seq[PathElement], value: Any, inclusive: Boolean) extends GeoJsonQuery {
    override def toFilter(idPath: Option[Seq[PathElement]], dtgPath: Option[Seq[PathElement]]): Filter = {
      if (inclusive) {
        ff.lessOrEqual(filterAttribute(path, dtgPath), ff.literal(value))
      } else {
        ff.less(filterAttribute(path, dtgPath), ff.literal(value))
      }
    }
    override def toString =
      s"""{"${JsonPathParser.print(path, dollar = false)}":{"$$${if (inclusive) "lte" else "lt"}":${printJson(value)}}}"""
  }

  object LessThan {
    def apply(path: String, value: Any, inclusive: Boolean = false): LessThan =
      LessThan(JsonPathParser.parse(path), value, inclusive)
  }

  /**
    * Greater than comparison
    *
    * @param path property to evaluate
    * @param value value to compare with property value
    * @param inclusive inclusive bounds
    */
  case class GreaterThan(path: Seq[PathElement], value: Any, inclusive: Boolean) extends GeoJsonQuery {
    override def toFilter(idPath: Option[Seq[PathElement]], dtgPath: Option[Seq[PathElement]]): Filter = {
      if (inclusive) {
        ff.greaterOrEqual(filterAttribute(path, dtgPath), ff.literal(value))
      } else {
        ff.greater(filterAttribute(path, dtgPath), ff.literal(value))
      }
    }
    override def toString =
      s"""{"${JsonPathParser.print(path, dollar = false)}":{"$$${if (inclusive) "gte" else "gt"}":${printJson(value)}}}"""
  }

  object GreaterThan {
    def apply(path: String, value: Any, inclusive: Boolean = false): GreaterThan =
      GreaterThan(JsonPathParser.parse(path), value, inclusive)
  }

  /**
    * Intersection of multiple filters
    *
    * @param children filters to intersect
    */
  case class And(children: GeoJsonQuery*) extends GeoJsonQuery {
    override def toFilter(idPath: Option[Seq[PathElement]], dtgPath: Option[Seq[PathElement]]): Filter = {
      import scala.collection.JavaConversions._
      ff.and(children.map(_.toFilter(idPath, dtgPath)))
    }
    override def toString = children.map(_.toString).map(s => s.substring(1, s.length - 1)).mkString("{", ",", "}")
  }

  /**
    * Union of multiple filters
    *
    * @param children filters to union
    */
  case class Or(children: GeoJsonQuery*) extends GeoJsonQuery {
    override def toFilter(idPath: Option[Seq[PathElement]], dtgPath: Option[Seq[PathElement]]): Filter = {
      import scala.collection.JavaConversions._
      ff.or(children.map(_.toFilter(idPath, dtgPath)))
    }
    override def toString = children.mkString("""{"$or":[""", ",", "]}")
  }
}