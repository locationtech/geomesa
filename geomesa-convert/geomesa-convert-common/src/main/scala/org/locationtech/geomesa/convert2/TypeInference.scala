/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert2.transforms.DateFunctionFactory.StandardDateParser
import org.locationtech.geomesa.convert2.transforms.TransformerFunction
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, ObjectType, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.{DateParsing, WKTUtils}
import org.locationtech.jts.geom._

import java.lang.{Boolean => jBoolean, Double => jDouble, Float => jFloat, Long => jLong}
import java.util.{Date, Locale}
import scala.util.Try

object TypeInference {

  import ObjectType._

  import scala.collection.JavaConverters._

  private val geometries =
    Seq(POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRY_COLLECTION, GEOMETRY)

  // fields to match for lat/lon - in priority order, from most to least specific
  private val latitudeNames = Seq("latitude", "lat")
  private val longitudeNames = Seq("longitude", "lon", "long")

  // map of priorities and transforms for merging different number types
  private val mergeUpNumbers = Map(
    DOUBLE -> (0, CastToDouble),
    FLOAT  -> (1, CastToFloat),
    LONG   -> (2, CastToLong),
    INT    -> (3, CastToInt),
  )

  /**
   * Infer types
   *
   * @param pathsAndValues paths (xpath, jsonpath, etc) to values
   * @param sft simple feature type, or name for inferred type
   * @param namer naming for inferred attributes
   * @param failureRate allowed failure rate for conversion to a given type
   * @return
   */
  def infer(
      pathsAndValues: Seq[PathWithValues],
      sft: Either[String, SimpleFeatureType],
      namer: String => String = new Namer(),
      failureRate: Float = 0.1f): InferredTypesWithPaths = {
    val inferred = pathsAndValues.map { case PathWithValues(path, values) =>
      val rawTypes = values.map(InferredType.infer)
      // merge the types for this column from each row
      val typed = merge(rawTypes.toSeq, failureRate)
      val baseName = if (path.nonEmpty) { path } else { String.valueOf(typed.typed).toLowerCase(Locale.US) }
      TypeWithPath(path, typed.copy(name = namer(baseName)))
    }
    val geom = deriveGeometry(inferred.map(_.inferredType), namer).map(TypeWithPath("", _))

    val typesWithPaths = inferred ++ geom

    sft match {
      case Left(name) =>
        val schema = TypeInference.schema(name, typesWithPaths.map(_.inferredType))
        InferredTypesWithPaths(schema, typesWithPaths)

      case Right(sft) =>
        // validate the existing schema
        AbstractConverterFactory.validateInferredType(sft, typesWithPaths.map(_.inferredType.typed))
        val renamed = typesWithPaths.zip(sft.getAttributeDescriptors.asScala.map(_.getLocalName)).map {
          case (inferred, name) => inferred.copy(inferredType = inferred.inferredType.copy(name = name))
        }
        InferredTypesWithPaths(sft, renamed)
    }
  }

  /**
    * Try to derive a geometry field, if one does not already exist
    *
    * @param types known types
    * @return
    */
  def deriveGeometry(types: Seq[InferredType], namer: String => String): Option[InferredType] = {
    // if there is no geometry field, see if we can derive one
    if (types.map(_.typed).exists(geometries.contains)) { None } else {
      val nums = types.filter(t => t.typed == ObjectType.DOUBLE || t.typed == ObjectType.FLOAT)
      if (nums.lengthCompare(2) < 0) { None } else {
        def findBestMatch(names: Seq[String]): Option[InferredType] = {
          // determine priority order
          val potentials = nums.flatMap { t =>
            val name = t.name.toLowerCase(Locale.US)
            val i = names.indexOf(name)
            val j = names.indexWhere(n => name.endsWith(s"_$n"))
            if (i != -1) {
              Some(i -> t)
            } else if (j != -1) {
              Some(j + names.length -> t) // add names.length to make exact matches sort first
            } else {
              None
            }
          }
          potentials.sortBy(_._1).collectFirst { case (_, t) => t }
        }
        for {
          lat <- findBestMatch(latitudeNames)
          lon <- findBestMatch(longitudeNames)
        } yield {
          InferredType(namer("geom"), POINT, DerivedTransform("point", lon.name, lat.name))
        }
      }
    }
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
      spec.append(s";${SimpleFeatureTypes.Configs.MixedGeometries}=true")
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
      Some(InferredType("", GEOMETRY, IdentityTransform))
    } else {
      for {
        (leftPriority, leftTransform) <- mergeUpNumbers.get(left.typed)
        (rightPriority, rightTransform) <- mergeUpNumbers.get(right.typed)
      } yield {
        if (leftPriority <= rightPriority) {
          left.copy(transform = leftTransform)
        } else {
          right.copy(transform = rightTransform)
        }
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
    case STRING | null       => "String"
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
   * Helper for naming attributes during type inference
   */
  class Namer(existing: Seq[String] = Seq.empty) extends (String => String) {

    // track the names we use for each column to ensure no duplicates
    private val uniqueNames = scala.collection.mutable.HashSet[String](existing: _*)

    /**
     * Get a valid attribute name based on the element name
     *
     * @param key json key
     * @return
     */
    override def apply(key: String): String = {
      val base = key.replaceAll("[^A-Za-z0-9]+", "_").replaceAll("^_|_$", "")
      var candidate = base
      var i = 0
      while (FeatureUtils.ReservedWords.contains(candidate.toUpperCase(Locale.US)) || !uniqueNames.add(candidate)) {
        candidate = s"${base}_$i"
        i += 1
      }
      candidate
    }
  }

  /**
   * Path to an attribute (xpath, json path, column number) and the values of that attribute
   *
   * @param path path
   * @param values sample values
   */
  case class PathWithValues(path: String, values: Iterable[Any])

  /**
   * Inferred feature type
   *
   * @param sft simple feature type
   * @param types types and paths to the attributes
   */
  case class InferredTypesWithPaths(sft: SimpleFeatureType, types: Seq[TypeWithPath])

  /**
   * An attribute type with a path to the attribute (xpath, json path, column number, etc)
   *
   * @param path path
   * @param inferredType type
   */
  case class TypeWithPath(path: String, inferredType: InferredType)

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
    def apply(i: Int): String = s"$to($$$i)"
  }

  case object CastToInt extends CastTransform("toInt")
  case object CastToLong extends CastTransform("toLong")
  case object CastToFloat extends CastTransform("toFloat")
  case object CastToDouble extends CastTransform("toDouble")
  case object CastToBoolean extends CastTransform("toBoolean")
  case object CastToString extends CastTransform("toString")

  case class FunctionTransform(prefix: String, suffix: String) extends InferredTransform {
    def apply(i: Int): String = s"$prefix$$$i$suffix"
  }

  // function transform that only operates on derived fields and not $i
  case class DerivedTransform(name: String, fields: String*) extends InferredTransform {
    def apply(i: Int): String = s"$name${fields.mkString("($", ",$", ")")}"
  }

  object InferredType {

    private val dateParsers = TransformerFunction.functions.values.collect { case f: StandardDateParser => f }.toArray

    /**
      * Infer a type from a value. Returned type will have an empty name
      *
      * @param value value
      * @return
      */
    def infer(value: Any): InferredType = {
      value match {
        case null | ""                => InferredType("", null, IdentityTransform)
        case _: Int | _: Integer      => InferredType("", INT, IdentityTransform)
        case _: Long | _: jLong       => InferredType("", LONG, IdentityTransform)
        case _: Float | _: jFloat     => InferredType("", FLOAT, IdentityTransform)
        case _: Double | _: jDouble   => InferredType("", DOUBLE, IdentityTransform)
        case _: Boolean | _: jBoolean => InferredType("", BOOLEAN, IdentityTransform)
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
        case _: java.util.List[_]     => InferredType("", LIST, IdentityTransform)

        case s: String =>
          val trimmed = s.trim
          tryNumberParsing(trimmed)
              .orElse(tryDateParsing(trimmed))
              .orElse(tryGeometryParsing(trimmed))
              .orElse(tryBooleanParsing(trimmed))
              .orElse(tryUuidParsing(trimmed))
              .getOrElse(InferredType("", STRING, IdentityTransform))

        case _ => InferredType("", STRING, CastToString)
      }
    }

    private def tryNumberParsing(s: String): Option[InferredType] = {
      Try(BigDecimal(s)).toOption.collect {
        case n if s.indexOf('.') == -1 && n.isValidInt  => InferredType("", INT, CastToInt)
        case n if s.indexOf('.') == -1 && n.isValidLong => InferredType("", LONG, CastToLong)
        // don't consider floats - even valid floats cause rounding changes when using as doubles in geometries
        case n if n.isDecimalDouble                     => InferredType("", DOUBLE, CastToDouble)
      }
    }

    private def tryDateParsing(s: String): Option[InferredType] = {
      var i = 0
      while (i < dateParsers.length) {
        val p = dateParsers(i)
        if (Try(DateParsing.parseDate(s, p.format)).isSuccess) {
          return Some(InferredType("", DATE, FunctionTransform(s"${p.names.head}(", ")")))
        }
        i += 1
      }
      None
    }

    private def tryGeometryParsing(s: String): Option[InferredType] = {
      Try(WKTUtils.read(s)).toOption.map {
        case _: Point              => InferredType("", POINT, FunctionTransform("point(", ")"))
        case _: LineString         => InferredType("", LINESTRING, FunctionTransform("linestring(", ")"))
        case _: Polygon            => InferredType("", POLYGON, FunctionTransform("polygon(", ")"))
        case _: MultiPoint         => InferredType("", MULTIPOINT, FunctionTransform("multipoint(", ")"))
        case _: MultiLineString    => InferredType("", MULTILINESTRING, FunctionTransform("multilinestring(", ")"))
        case _: MultiPolygon       => InferredType("", MULTIPOLYGON, FunctionTransform("multipolygon(", ")"))
        case _: GeometryCollection => InferredType("", GEOMETRY_COLLECTION, FunctionTransform("geometrycollection(", ")"))
        case _                     => InferredType("", GEOMETRY, FunctionTransform("geometry(", ")"))
      }
    }

    private def tryBooleanParsing(s: String): Option[InferredType] =
      Try(s.toBoolean).toOption.map(_ => InferredType("", BOOLEAN, CastToBoolean))

    private def tryUuidParsing(s: String): Option[InferredType] =
      Try(java.util.UUID.fromString(s)).toOption.map(_ => InferredType("", UUID, IdentityTransform))
  }
}
