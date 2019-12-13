/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geojson.query

import java.io.ByteArrayOutputStream

import org.locationtech.jts.geom.Geometry
import org.geotools.geojson.geom.GeometryJSON
import org.json4s.{JArray, JObject, JValue}
import org.locationtech.geomesa.features.kryo.json.JsonPathParser
import org.opengis.filter.Filter

import scala.util.control.NonFatal

/**
  * Query trait
  */
sealed trait GeoJsonQuery {
  def toFilter(propertyTransformer: PropertyTransformer): Filter
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

  private[query] val defaultGeom = "geom"

  private val jsonGeometry = new GeometryJSON()

  /**
    * Parse a query string
    *
    * @param jsonValue json query value
    * @return
    */
  def apply(jsonValue: JValue): GeoJsonQuery = {
    jsonValue match {
      case j: JObject => evaluate(j)
      case _ => throw new IllegalArgumentException("Invalid input - expected JSON object")
    }
  }

  /**
    * Parse a query string
    *
    * @param query query string
    * @return
    */
  def apply(query: String): GeoJsonQuery = {
    import org.json4s._
    import org.json4s.native.JsonMethods._

    if (query == null || query.isEmpty) {
      Include
    } else {
      try {
        apply(parse(query))
      } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid query string:\n$query", e)
      }
    }
  }

  def checkJsonPaths(str: String): String = {
    if (str.startsWith("$.")) {
      JsonPathParser.print(JsonPathParser.parse(str))
    } else {
      str
    }
  }

  /**
    * Create a query from a parsed json object
    *
    * @param json query json
    * @return
    */
  private def evaluate(json: JObject): GeoJsonQuery = {
    if (json.obj.isEmpty) {
      Include
    } else {
      val predicates = json.obj.map {
        case (prop, v: JObject) =>
          evaluatePredicate(checkJsonPaths(prop), v)

        case ("$or", v: JArray) =>
          val ors = v.arr.map {
            case o: JObject => evaluate(o)
            case o => throw new IllegalArgumentException(s"Expected Object, got $o")
          }
          Or(ors: _*)

        case (prop, v: JValue) =>
          Equals(checkJsonPaths(prop), v.values)
      }
      if (predicates.length > 1) {
        And(predicates: _*)
      } else {
        predicates.head
      }
    }
  }

  /**
    * Evaluate a complex predicate
    *
    * @param prop property that the predicate operates on
    * @param json predicate json object
    * @return
    */
  private def evaluatePredicate(prop: String, json: JObject): GeoJsonQuery = {
    json.obj.headOption match {
      case Some(("$bbox", v: JArray)) =>
        // { "$bbox" : [-180, -90, 180, 90] }
        val List(xmin, ymin, xmax, ymax) = v.values.asInstanceOf[List[Number]]
        Bbox(prop, xmin.doubleValue, ymin.doubleValue, xmax.doubleValue, ymax.doubleValue)

      case Some(("$intersects", v: JObject)) =>
        // { "$intersects" : { "$geometry" : { "type" : "Point", "coordinates" : [30, 10] } } }
        Intersects(prop, evaluateGeometry(v))

      case Some(("$within", v: JObject)) =>
        // { "$within" : { "$geometry" : { "type" : "Polygon", "coordinates": [ [ [0,0], [3,6], [6,1], [0,0] ] ] } } }
        Within(prop, evaluateGeometry(v))

      case Some(("$dwithin", v: JObject)) =>
        // { "$dwithin" : { "$geometry" : { "type" : "Point", "coordinates" : [30, 10] }, "$dist" : 100.50, "$unit" : "feet" } }
        val (dist, unit) = evaluateDwithin(v)
        Dwithin(prop, evaluateGeometry(v), dist, unit)

      case Some(("$contains", v: JObject)) =>
        // { "$contains" : { "$geometry" : { "type" : "Point", "coordinates" : [30, 10] } } }
        Contains(prop, evaluateGeometry(v))

      case Some(("$lt", v)) =>
        // { "$lt" : 10 }
        LessThan(prop, v.values, inclusive = false)

      case Some(("$lte", v)) =>
        // { "$lte" : 10 }
        LessThan(prop, v.values, inclusive = true)

      case Some(("$gt", v)) =>
        // { "$gt" : 10 }
        GreaterThan(prop, v.values, inclusive = false)

      case Some(("$gte", v)) =>
        // { "$gte" : 10 }
        GreaterThan(prop, v.values, inclusive = true)

      case Some((p, _)) =>
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
    * Read a json geometry object:
    * { "$geometry" : { "type" : "Point", "coordinates" : [30, 10] } }
    *
    * @param json geometry object
    * @return
    */
  private def evaluateDwithin(json: JObject): (Double, String) = {
    import org.json4s._
    implicit val formats = DefaultFormats
    val distance = json.obj.find(_._1 == "$dist").map(_._2).getOrElse {
      throw new IllegalArgumentException(s"Expected $$dist, got ${json.obj.map(_._1).mkString(", ")}")
    }.extract[Double]

    val unit = json.obj.find(_._1 == "$unit").map(_._2).getOrElse {
      throw new IllegalArgumentException(s"Expected $$unit, got ${json.obj.map(_._1).mkString(", ")}")
    }.extract[String]

    (distance, unit)
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
    override def toFilter(propertyTransformer: PropertyTransformer): Filter = Filter.INCLUDE

    override val toString = "{}"
  }

  /**
    * Spatial intersect
    *
    * @param prop     property to evaluate
    * @param geometry geometry to compare with property value
    */
  case class Intersects(prop: String, geometry: Geometry) extends GeoJsonQuery {
    override def toFilter(propertyTransformer: PropertyTransformer): Filter =
      ff.intersects(ff.property(propertyTransformer.transform(prop)), ff.literal(geometry))

    override def toString =
      s"""{"$prop":{"$$intersects":${printJson(geometry)}}}"""
  }

  object Intersects {
    def apply(geometry: Geometry): Intersects = Intersects(defaultGeom, geometry)
  }

  /**
    * Spatial within
    *
    * @param prop     property to evaluate
    * @param geometry geometry to compare with property value
    */
  case class Within(prop: String, geometry: Geometry) extends GeoJsonQuery {
    override def toFilter(propertyTransformer: PropertyTransformer): Filter =
      ff.within(ff.property(propertyTransformer.transform(prop)), ff.literal(geometry))

    override def toString =
      s"""{"$prop":{"$$within":${printJson(geometry)}}}"""
  }

  object Within {
    def apply(geometry: Geometry): Within = Within(defaultGeom, geometry)
  }

  /**
    * Spatial contains
    *
    * @param prop     property to evaluate
    * @param geometry geometry to compare with property value
    */
  case class Contains(prop: String, geometry: Geometry) extends GeoJsonQuery {
    override def toFilter(propertyTransformer: PropertyTransformer): Filter =
      ff.contains(ff.property(propertyTransformer.transform(prop)), ff.literal(geometry))

    override def toString =
      s"""{"$prop":{"$$contains":${printJson(geometry)}}}"""
  }

  object Contains {
    def apply(geometry: Geometry): Contains = Contains(defaultGeom, geometry)
  }

  /**
    * Spatial dwithin
    *
    * @param prop     property to evaluate
    * @param geometry geometry to compare with property value
    * @param dist     the max distance between geometries
    * @param units    the units of distance (feet, meters, statute miles, kilometers)
    */
  case class Dwithin(prop: String, geometry: Geometry, dist: Double, units: String) extends GeoJsonQuery {
    override def toFilter(propertyTransformer: PropertyTransformer): Filter =
      ff.dwithin(ff.property(propertyTransformer.transform(prop)), ff.literal(geometry), dist, units)

    override def toString =
      s"""{"$prop":{"$$dwithin":${printJson(geometry)}, "$$dist":$dist, "$$unit":"$units"}}"""
  }

  object Dwithin {
    def apply(geometry: Geometry, distance: Double, units: String): Dwithin =
      Dwithin(defaultGeom, geometry, distance, units)
  }


  /**
    * Spatial bounding box
    *
    * @param prop property to evaluate
    * @param xmin min x value
    * @param ymin min y value
    * @param xmax max x value
    * @param ymax max y value
    */
  case class Bbox(prop: String, xmin: Double, ymin: Double, xmax: Double, ymax: Double) extends GeoJsonQuery {
    override def toFilter(propertyTransformer: PropertyTransformer): Filter =
      ff.bbox(ff.property(propertyTransformer.transform(prop)), xmin, ymin, xmax, ymax, "4326")

    override def toString = s"""{"$prop":{"$$bbox":[$xmin,$ymin,$xmax,$ymax]}}"""
  }

  object Bbox {
    def apply(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Bbox =
      Bbox(defaultGeom, xmin, ymin, xmax, ymax)
  }

  /**
    * Equality
    *
    * @param prop  property to evaluate
    * @param value value to compare with property value
    */
  case class Equals(prop: String, value: Any) extends GeoJsonQuery {
    override def toFilter(propertyTransformer: PropertyTransformer): Filter = {
      // Equals is the only property that can act on feature ids
      if (propertyTransformer.useFid(prop)) {
        val fids = value match {
          case v: Iterable[_] => v.map(_.toString).toSeq
          case v => Seq(v.toString)
        }
        ff.id(fids.map(ff.featureId): _*)
      }
      else {
        ff.equals(ff.property(propertyTransformer.transform(prop)), ff.literal(value))
      }
    }

    override def toString = s"""{"$prop":${printJson(value)}}"""
  }

  /**
    * Less than comparison
    *
    * @param prop      property to evaluate
    * @param value     value to compare with property value
    * @param inclusive inclusive bounds
    */
  case class LessThan(prop: String, value: Any, inclusive: Boolean) extends GeoJsonQuery {
    override def toFilter(propertyTransformer: PropertyTransformer): Filter = {
      if (inclusive) {
        ff.lessOrEqual(ff.property(propertyTransformer.transform(prop)), ff.literal(value))
      } else {
        ff.less(ff.property(propertyTransformer.transform(prop)), ff.literal(value))
      }
    }

    override def toString =
      s"""{"$prop":{"$$${if (inclusive) "lte" else "lt"}":${printJson(value)}}}"""
  }

  /**
    * Greater than comparison
    *
    * @param prop      property to evaluate
    * @param value     value to compare with property value
    * @param inclusive inclusive bounds
    */
  case class GreaterThan(prop: String, value: Any, inclusive: Boolean) extends GeoJsonQuery {
    override def toFilter(propertyTransformer: PropertyTransformer): Filter = {
      if (inclusive) {
        ff.greaterOrEqual(ff.property(propertyTransformer.transform(prop)), ff.literal(value))
      } else {
        ff.greater(ff.property(propertyTransformer.transform(prop)), ff.literal(value))
      }
    }

    override def toString =
      s"""{"$prop":{"$$${if (inclusive) "gte" else "gt"}":${printJson(value)}}}"""
  }

  /**
    * Intersection of multiple filters
    *
    * @param children filters to intersect
    */
  case class And(children: GeoJsonQuery*) extends GeoJsonQuery {
    override def toFilter(propertyTransformer: PropertyTransformer): Filter = {
      import scala.collection.JavaConversions._
      ff.and(children.map(_.toFilter(propertyTransformer)))
    }

    override def toString = children.map(_.toString).map(s => s.substring(1, s.length - 1)).mkString("{", ",", "}")
  }

  /**
    * Union of multiple filters
    *
    * @param children filters to union
    */
  case class Or(children: GeoJsonQuery*) extends GeoJsonQuery {
    override def toFilter(propertyTransformer: PropertyTransformer): Filter = {
      import scala.collection.JavaConversions._
      ff.or(children.map(_.toFilter(propertyTransformer)))
    }

    override def toString = children.mkString("""{"$or":[""", ",", "]}")
  }

}