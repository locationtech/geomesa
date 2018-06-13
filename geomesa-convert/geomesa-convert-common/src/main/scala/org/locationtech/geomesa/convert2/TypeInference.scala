/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import java.lang.{Boolean => jBoolean, Double => jDouble, Float => jFloat, Long => jLong}
import java.util.{Date, Locale}

import com.vividsolutions.jts.geom._
import org.locationtech.geomesa.convert2.transforms.DateFunctionFactory.StandardDateParser
import org.locationtech.geomesa.convert2.transforms.TransformerFunction
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.{DateParsing, WKTUtils}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.{Success, Try}

object TypeInference {

  import ObjectType._

  private val geometries =
    Seq(POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRY_COLLECTION, GEOMETRY)

  private val latitudeNames = Seq("lat", "latitude")
  private val longitudeNames = Seq("lon", "long", "longitude")

  /**
    * Infer attribute types based on input data
    *
    * @param data rows of columns
    * @param failureRate per column, max percentage of values that can fail a conversion to a particular type,
    *                    and still consider the column to be that type
    * @return
    */
  def infer(data: Iterable[Iterable[Any]],
            names: Iterable[String] = Seq.empty,
            failureRate: Float = 0.1f): IndexedSeq[InferredType] = {
    // list of inferred types for each column
    val rawTypes = ArrayBuffer.empty[ListBuffer[InferredType]]
    var i = 0
    data.iterator.foreach { row =>
      // skip empty rows or rows consisting of a single whitespace-only string
      if (row.nonEmpty && (row.size > 1 || row.headOption.collect { case s: String if s.trim.isEmpty => s }.isEmpty)) {
        i = 0
        row.foreach { col =>
          if (i == rawTypes.length) {
            // grow our column array if needed, we don't assume all rows have the same number of columns
            rawTypes += ListBuffer.empty[InferredType]
          }
          // add the inferred type for this column value - it won't have a name at this point
          rawTypes(i) += InferredType.infer(col)
          i += 1
        }
      }
    }

    // track the names we use for each column to ensure no duplicates
    val uniqueNames = scala.collection.mutable.HashSet.empty[String]
    // names that were provided
    val nameIterator = names.iterator

    val types = rawTypes.map { raw =>

      // merge the types for this column from each row
      val typ = merge(raw, failureRate)

      // determine the column name
      var base: String = null
      var name: String = null
      var i = 0

      if (nameIterator.hasNext) {
        // if the name was passed in, start with that
        base = nameIterator.next
        name = base
      } else {
        // use the type with an _i, e.g. int_0, string_1
        base = typ.typed.toString.toLowerCase(Locale.US)
        name = s"${base}_0"
        i += 1
      }

      while (FeatureUtils.ReservedWords.contains(name.toUpperCase(Locale.US)) || !uniqueNames.add(name)) {
        name = s"${base}_$i"
        i += 1
      }

      // set the name for the column
      typ.copy(name = name)
    }

    // if there is no geometry field, see if we can derive one
    if (types.lengthCompare(1) > 0 && !types.map(_.typed).exists(geometries.contains)) {
      var name = "geom"
      var i = 0
      while (!uniqueNames.add(name)) {
        name = s"geom_$i"
        i += 1
      }

      var lat, lon: String = null

      // if we have lat/lon columns, use those
      types.exists { t =>
        val name = t.name.toLowerCase(Locale.US)
        if (latitudeNames.contains(name)) {
          lat = t.name
        } else if (longitudeNames.contains(name)) {
          lon = t.name
        }
        // stop iterating if we find them both
        lat != null && lon != null
      }

      if (lat == null || lon == null) {
        // as a fallback, check for 2 consecutive floats or doubles - assume longitude first
        // note that this is pretty brittle, but hopefully better than nothing
        i = 1
        while (i < types.length) {
          val left = types(i - 1)
          val right = types(i)
          if ((left.typed == FLOAT || left.typed == DOUBLE) && left.typed == right.typed) {
            lon = left.name
            lat = right.name
            i = types.length // break out of the loop
          }
          i += 1
        }
      }

      if (lat != null && lon != null) {
        types += InferredType(name, POINT, DerivedTransform("point", lon, lat))
      }
    }

    types
  }

  /**
    * Create a simple feature type from inferred types
    *
    * @param name sft name
    * @param types types
    * @return
    */
  def schema(name: String, types: Seq[InferredType]): SimpleFeatureType = {
    val spec = new StringBuilder()
    types.foreach { typ =>
      if (spec.nonEmpty) {
        spec.append(",")
      }
      spec.append(typ.name).append(':').append(typ.binding)
    }
    if (types.exists(_.typed == ObjectType.GEOMETRY)) {
      spec.append(s";${SimpleFeatureTypes.Configs.MIXED_GEOMETRIES}=true")
    }
    SimpleFeatureTypes.createType(name, spec.toString())
  }

  /**
    * Merge the values from a given column into a single type
    *
    * @param types types extracted from the column
    * @param failureRate max percentage of values that can fail a conversion to a particular type,
    *                    and still consider the column to be that type
    * @return
    */
  private def merge(types: Seq[InferredType], failureRate: Float): InferredType = {

    val typeCounts = scala.collection.mutable.Map.empty[InferredType, Int].withDefaultValue(0)
    types.foreach(t => typeCounts(t) += 1)

    if (typeCounts.size == 1) {
      // if only one type, we're good
      return types.head
    }

    var (mostFrequentType, mostFrequentTypeCount) = typeCounts.maxBy(_._2)

    if (1f - mostFrequentTypeCount.toFloat / types.length < failureRate) {
      // if the most frequent type passes the error threshold, use it
      return mostFrequentType
    }

    // try to combine types up into more general ones
    val mergedTypeCounts = typeCounts.foldLeft(Map.empty[InferredType, Int]) { case (counts, (typ, count)) =>
      val merged = scala.collection.mutable.Map.empty[InferredType, (InferredType, Int)] // from -> to
      counts.foreach { case (mergedType, mergedCount) =>
        merge(typ, mergedType).foreach(t => merged += mergedType -> (t, mergedCount + count))
      }
      if (merged.isEmpty) {
        counts + (typ -> count)
      } else {
        (counts -- merged.keys) ++ merged.values
      }
    }

    mergedTypeCounts.maxBy(_._2) match {
      case (t, c) => mostFrequentType = t; mostFrequentTypeCount = c
    }

    if (1f - mostFrequentTypeCount.toFloat / types.length < failureRate) {
      // if the most frequent merged type passes the error threshold, use it
      return mostFrequentType
    }

    // if nothing else, fall back to string
    InferredType("", STRING, CastToString)
  }

  /**
    * Merge two types into a single super-type, if one exists
    *
    * for example:
    *
    *   merge(float, double) == double
    *   merge(point, linestring) == geometry
    *
    * @param left first type
    * @param right second type
    * @return
    */
  private def merge(left: InferredType, right: InferredType): Option[InferredType] = {
    // string + foo => string, null + foo => foo
    if (left == right || left.typed == STRING || right.typed == null) {
      Some(left)
    } else if (left.typed == null || right.typed == STRING) {
      Some(right)
    } else if (geometries.contains(left.typed) && geometries.contains(right.typed)) {
      Some(InferredType("", GEOMETRY, FunctionTransform("geometry")))
    } else {
      left.typed match {
        case INT    if Seq(LONG, FLOAT, DOUBLE).contains(right.typed) => Some(right)
        case LONG   if right.typed == INT                             => Some(left)
        case LONG   if Seq(FLOAT, DOUBLE).contains(right.typed)       => Some(right)
        case FLOAT  if Seq(INT, LONG).contains(right.typed)           => Some(left)
        case FLOAT  if right.typed == DOUBLE                          => Some(right)
        case DOUBLE if Seq(INT, LONG, FLOAT).contains(right.typed)    => Some(left)
        case _ => None
      }
    }
  }

  /**
    * Get the simple feature type spec binding for a type
    *
    * @param typed object type
    * @return
    */
  private def binding(typed: ObjectType): String = typed match {
    case STRING              => "String"
    case INT                 => "Int"
    case LONG                => "Long"
    case FLOAT               => "Float"
    case DOUBLE              => "Double"
    case BOOLEAN             => "Boolean"
    case DATE                => "Date"
    case UUID                => "UUID"
    case LIST                => "List"
    case MAP                 => "Map"
    case BYTES               => "Bytes"
    case JSON                => "String:json=true"
    case POINT               => "Point:srid=4326"
    case LINESTRING          => "LineString:srid=4326"
    case POLYGON             => "Polygon:srid=4326"
    case MULTIPOINT          => "MultiPoint:srid=4326"
    case MULTILINESTRING     => "MultiLineString:srid=4326"
    case MULTIPOLYGON        => "MultiPolygon:srid=4326"
    case GEOMETRY_COLLECTION => "GeometryCollection:srid=4326"
    case GEOMETRY            => "Geometry:srid=4326"
  }

  /**
    * Inferred type of a converter field
    *
    * @param name name of the field
    * @param typed type of the field
    * @param transform converter transform
    */
  case class InferredType(name: String, typed: ObjectType, transform: InferredTransform) {
    def binding: String = TypeInference.binding(typed)
  }

  /**
    * Inferred converter transform
    */
  sealed trait InferredTransform {

    /**
      * Get the converter transform as a string, suitable for the 'transform' field of a converter field definition
      *
      * @param i the index of the raw value in the transform arguments array.
      *          for delimited text, this would generally be the column number.
      *          for json, it would generally be '0', since the json-path result is placed at index 0
      * @return
      */
    def apply(i: Int): String
  }

  case object IdentityTransform extends InferredTransform {
    def apply(i: Int): String = s"$$$i"
  }

  sealed class CastTransform(to: String) extends InferredTransform {
    def apply(i: Int): String = s"$$$i::$to"
  }

  case object CastToInt extends CastTransform("int")
  case object CastToLong extends CastTransform("long")
  case object CastToFloat extends CastTransform("float")
  case object CastToDouble extends CastTransform("double")
  case object CastToBoolean extends CastTransform("boolean")
  case object CastToString extends CastTransform("string")

  // note: assumes args come after $i in the function definition
  case class FunctionTransform(name: String, args: Any*) extends InferredTransform {
    def apply(i: Int): String = s"$name${args.+:(i).mkString("($", ",", ")")}"
  }

  // function transform that only operates on derived fields and not $i
  case class DerivedTransform(name: String, fields: String*) extends InferredTransform {
    def apply(i: Int): String = s"$name${fields.mkString("($", ",$", ")")}"
  }

  object InferredType {

    private val dateParsers = TransformerFunction.functions.values.collect { case f: StandardDateParser => f }.toSeq

    /**
      * Infer a type from a value. Returned type will have an empty name
      *
      * @param value value
      * @return
      */
    def infer(value: Any): InferredType = {
      value match {
        case null | ""                => InferredType("", null, IdentityTransform)
        case _: Int | _: Integer      => InferredType("", INT, CastToInt)
        case _: Long | _: jLong       => InferredType("", LONG, CastToLong)
        case _: Float | _: jFloat     => InferredType("", FLOAT, CastToFloat)
        case _: Double | _: jDouble   => InferredType("", DOUBLE, CastToDouble)
        case _: Boolean | _: jBoolean => InferredType("", BOOLEAN, CastToBoolean)
        case _: Date                  => InferredType("", DATE, IdentityTransform)
        case _: Array[Byte]           => InferredType("", BYTES, IdentityTransform)
        case _: java.util.UUID        => InferredType("", UUID, IdentityTransform)
        case _: Point                 => InferredType("", POINT, IdentityTransform)
        case _: LineString            => InferredType("", LINESTRING, IdentityTransform)
        case _: Polygon               => InferredType("", POLYGON, IdentityTransform)
        case _: MultiPoint            => InferredType("", MULTIPOINT, IdentityTransform)
        case _: MultiLineString       => InferredType("", MULTILINESTRING, IdentityTransform)
        case _: MultiPolygon          => InferredType("", MULTIPOLYGON, IdentityTransform)
        case _: GeometryCollection    => InferredType("", GEOMETRY_COLLECTION, IdentityTransform)

        case s: String =>
          val trimmed = s.trim
          tryNumberParsing(trimmed)
              .orElse(tryDateParsing(trimmed))
              .orElse(tryGeometryParsing(trimmed))
              .orElse(tryBooleanParsing(trimmed))
              .orElse(tryUuidParsing(trimmed))
              .getOrElse(InferredType("", STRING, CastToString))

        case _ =>
          InferredType("", STRING, CastToString)
      }
    }

    private def tryNumberParsing(s: String): Option[InferredType] = {
      Try(BigDecimal(s)).toOption.collect {
        case n if s.indexOf('.') == -1 && n.isValidInt  => InferredType("", INT, CastToInt)
        case n if s.indexOf('.') == -1 && n.isValidLong => InferredType("", LONG, CastToLong)
        case n if n.isDecimalFloat                      => InferredType("", FLOAT, CastToFloat)
        case n if n.isDecimalDouble                     => InferredType("", DOUBLE, CastToDouble)
      }
    }

    private def tryDateParsing(s: String): Option[InferredType] = {
      dateParsers.foreach { p =>
        if (Try(DateParsing.parseDate(s, p.format)).isSuccess) {
          return Some(InferredType("", DATE, FunctionTransform(p.names.head)))
        }
      }
      None
    }

    private def tryGeometryParsing(s: String): Option[InferredType] = {
      Try(WKTUtils.read(s)).toOption.map {
        case _: Point              => InferredType("", POINT, FunctionTransform("point"))
        case _: LineString         => InferredType("", LINESTRING, FunctionTransform("linestring"))
        case _: Polygon            => InferredType("", POLYGON, FunctionTransform("polygon"))
        case _: MultiPoint         => InferredType("", MULTIPOINT, FunctionTransform("multipoint"))
        case _: MultiLineString    => InferredType("", MULTILINESTRING, FunctionTransform("multilinestring"))
        case _: MultiPolygon       => InferredType("", MULTIPOLYGON, FunctionTransform("multipolygon"))
        case _: GeometryCollection => InferredType("", GEOMETRY_COLLECTION, FunctionTransform("geometrycollection"))
        case _                     => InferredType("", GEOMETRY, FunctionTransform("geometry"))
      }
    }

    private def tryBooleanParsing(s: String): Option[InferredType] =
      Try(s.toBoolean).toOption.map(_ => InferredType("", BOOLEAN, CastToBoolean))

    private def tryUuidParsing(s: String): Option[InferredType] =
      Try(java.util.UUID.fromString(s)).toOption.map(_ => InferredType("", UUID, IdentityTransform))
  }
}
