/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import java.lang.reflect.Type
import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}
import java.util.Date

import com.google.gson._
import org.locationtech.jts.geom.Geometry
import org.apache.commons.text.StringEscapeUtils
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.date.DateUtils.toInstant
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.geomesa.utils.date.DateUtils.toInstant
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * Stats used by the StatsIterator to compute various statistics server-side for a given query.
 */
trait Stat {

  type S <: Stat

  /**
    * The simple feature type that this stat operates on
    *
    * @return
    */
  def sft: SimpleFeatureType

  /**
   * Compute statistics based upon the given simple feature.
   * This method will be called for every SimpleFeature a query returns.
   *
   * @param sf feature to evaluate
   */
  def observe(sf: SimpleFeature): Unit

  /**
    * Tries to remove the given simple feature from the compiled statistics.
    * Note: may not be possible to un-observe a feature, in which case this method will
    * have no effect.
    *
    * @param sf feature to un-evaluate
    */
  def unobserve(sf: SimpleFeature): Unit

  /**
   * Add another stat to this stat. Avoids allocating another object.
   *
   * @param other the other stat to add
   */
  def +=(other: S): Unit

  /**
   * Non type-safe add - if stats are not the same type, will throw an exception
   *
   * @param other the other stat to add
   */
  def +=(other: Stat)(implicit d: DummyImplicit): Unit = this += other.asInstanceOf[S]

  /**
    * Combine two stats into a new stat
    *
    * @param other the other stat to add
    */
  def +(other: S): S

  /**
    * Non type-safe add - if stats are not the same type, will throw an exception
    *
    * @param other the other stat to add
    */
  def +(other: Stat)(implicit d: DummyImplicit): Stat = this + other.asInstanceOf[S]

  /**
   * Returns a JSON representation of the [[Stat]]
   *
   * @return stat as a json string
   */
  def toJson: String = Stat.JSON.toJson(toJsonObject)

  /**
    * Returns a representation of the [[Stat]] to be serialized
    *
    * This function should return a representation (view) of the [[Stat]] to be serialized as JSON.
    * Instances of [[Map]] can be used to represent JSON dictionaries or [[Seq]] for JSON arrays.
    * A [[collection.SortedMap]] such as [[collection.immutable.ListMap]] is recommended if key order
    * should be deterministic.  Other types may be used but could require the creation and registration
    * of custom serializers dependent on the JSON framework being utilized (currently [[Gson]]).
    *
    * @return stat as a json serializable object
    */
  def toJsonObject: Any

  /**
   * Necessary method used by the StatIterator. Indicates if the stat has any values or not
   *
   * @return true if stat contains values
   */
  def isEmpty: Boolean

  /**
    * Compares the two stats for equivalence. We don't use standard 'equals' as it gets messy with
    * mutable state and hash codes
    *
    * @param other other stat to compare
    * @return true if equals
    */
  def isEquivalent(other: Stat): Boolean

  /**
   * Clears the stat to its original state when first initialized.
   * Necessary method used by the StatIterator.
   */
  def clear(): Unit
}

/**
 * This class contains parsers which dictate how to instantiate a particular Stat.
 * Stats are created by passing a stats string as a query hint (QueryHints.STATS_STRING).
 *
 * A valid stats string should adhere to the parsers here:
 * e.g. "MinMax(attributeName);IteratorCount" or "RangeHistogram(attributeName,10,0,100)"
 * (see tests for more use cases)
 */
object Stat {

  private val ScalaMapSerializer = new JsonSerializer[Map[Any,Any]] {
    def serialize(s: Map[Any,Any], t: Type, jsc: JsonSerializationContext): JsonElement = jsc.serialize(s.asJava)
  }
  private val ScalaSeqSerializer = new JsonSerializer[Seq[Any]] {
    def serialize(s: Seq[Any], t: Type, jsc: JsonSerializationContext): JsonElement = jsc.serialize(s.asJava)
  }
  private val StatSerializer = new JsonSerializer[Stat] {
    def serialize(s: Stat, t: Type, jsc: JsonSerializationContext): JsonElement = jsc.serialize(s.toJsonObject)
  }
  private val GeometrySerializer = new JsonSerializer[Geometry] {
    def serialize(g: Geometry, t: Type, jsc: JsonSerializationContext): JsonElement =
      new JsonPrimitive(WKTUtils.write(g))
  }
  private val DateSerializer = new JsonSerializer[Date] {
    def serialize(d: Date, t: Type, jsc: JsonSerializationContext): JsonElement =
      new JsonPrimitive(GeoToolsDateFormat.format(toInstant(d)))
  }
  private val DoubleSerializer = new JsonSerializer[jDouble]() {
    def serialize(double: jDouble, t: Type, jsc: JsonSerializationContext): JsonElement = double match {
      /* NaN check, use null to mirror existing behavior for missing/invalid values */
      case d if jDouble.isNaN(d) => JsonNull.INSTANCE
      case d if d == jDouble.NEGATIVE_INFINITY => new JsonPrimitive("Infinity")
      case d if d == jDouble.POSITIVE_INFINITY => new JsonPrimitive("+Infinity")
      case _ => new JsonPrimitive(double)
    }
  }
  private val FloatSerializer = new JsonSerializer[jFloat]() {
    def serialize(float: jFloat, t: Type, jsc: JsonSerializationContext): JsonElement = float match {
      /* NaN check, use null to mirror existing behavior for missing/invalid values */
      case f if jFloat.isNaN(f) => JsonNull.INSTANCE
      case f if f == jFloat.NEGATIVE_INFINITY => new JsonPrimitive("Infinity")
      case f if f == jFloat.POSITIVE_INFINITY => new JsonPrimitive("+Infinity")
      case _ => new JsonPrimitive(float)
    }
  }

  private val JSON: Gson = new GsonBuilder()
    .serializeNulls()
    .registerTypeAdapter(classOf[Double], DoubleSerializer)
    .registerTypeAdapter(classOf[jDouble], DoubleSerializer)
    .registerTypeAdapter(classOf[Float], FloatSerializer)
    .registerTypeAdapter(classOf[jFloat], FloatSerializer)
    .registerTypeHierarchyAdapter(classOf[Stat], StatSerializer)
    .registerTypeHierarchyAdapter(classOf[Geometry], GeometrySerializer)
    .registerTypeHierarchyAdapter(classOf[Date], DateSerializer)
    .registerTypeHierarchyAdapter(classOf[Map[_,_]], ScalaMapSerializer)
    .registerTypeHierarchyAdapter(classOf[Seq[_]], ScalaSeqSerializer)
    .create()

  def apply(sft: SimpleFeatureType, s: String): Stat = StatParser.parse(sft, s)

  /**
    * String that will be parsed to a count stat
    *
    * @return
    */
  def Count(): String = "Count()"

  /**
    * String that will be parsed to a min/max stat
    *
    * @param attribute attribute name to min/max
    * @return
    */
  def MinMax(attribute: String): String = s"MinMax(${safeString(attribute)})"

  /**
    * String that will be parsed to a histogram stat
    *
    * @param attribute attribute name to histogram
    * @return
    */
  def Enumeration(attribute: String): String = s"Enumeration(${safeString(attribute)})"

  /**
    * String that will be parsed into a TopK stat
    *
    * @param attribute attribute name to evaluate
    * @return
    */
  def TopK(attribute: String): String = s"TopK(${safeString(attribute)})"

  /**
    * String that will be parsed into a count min sketch stat
    *
    * @param attribute attribute to sketch
    * @param precision precision of the sketch - @see Frequency
    * @return
    */
  def Frequency(attribute: String, precision: Int): String =
    s"Frequency(${safeString(attribute)},$precision)"

  /**
    * String that will be parsed into a count min sketch stat
    *
    * @param attribute attribute to sketch
    * @param dtg date attribute to use for binning
    * @param period time period to split on
    * @param precision precision of the sketch - @see Frequency
    * @return
    */
  def Frequency(attribute: String, dtg: String, period: TimePeriod, precision: Int): String =
    s"Frequency(${safeString(attribute)},${safeString(dtg)},$period,$precision)"

  /**
    * String that will be parsed into a z3 count min sketch stat
    *
    * @param geom geometry attribute
    * @param dtg date attribute
    * @param period time period to split on
    * @param precision precision of the z value - @see FrequencyZ3
    * @return
    */
  def Z3Frequency(geom: String, dtg: String, period: TimePeriod, precision: Int): String =
    s"Z3Frequency(${safeString(geom)},${safeString(dtg)},$period,$precision)"

  /**
    * String that will be parsed to a binned histogram stat
    *
    * @param attribute attribute name to histogram
    * @param bins the number of bins to create
    * @param min min value for the histogram
    * @param max max value for the histogram
    * @tparam T class type of the histogram attribute
    * @return
    */
  def Histogram[T](attribute: String, bins: Int, min: T, max: T)(implicit ct: ClassTag[T]): String = {
    val stringify = stringifier(ct.runtimeClass)
    s"Histogram(${safeString(attribute)},$bins,${safeString(stringify(min))},${safeString(stringify(max))})"
  }

  /**
    * String that will be parsed into a z3 range histogram stat
    *
    * @param geom geometry attribute
    * @param dtg date attribute
    * @param period time period to split on
    * @param length number of the bins per week - @see RangeHistogramZ3
    * @return
    */
  def Z3Histogram(geom: String, dtg: String, period: TimePeriod, length: Int): String =
    s"Z3Histogram(${safeString(geom)},${safeString(dtg)},$period,$length)"

  /**
    * String that will be parsed to a iterator stack counter
    *
    * @return
    */
  def IteratorStackCount(): String = "IteratorStackCount()"

  /**
    * String that will be parsed to a sequence of stat
    *
    * @param stats input strings that will be parsed as individual stats
    * @return
    */
  def SeqStat(stats: Seq[String]): String = stats.mkString(";")

  /**
    * Groups results by attribute and runs stats for each group.
    *
    * @param attribute attribute to group stats by
    * @param groupedStat stat to apply to grouped attributes
    * @return
    */
  def GroupBy(attribute: String, groupedStat: Stat): String = s"GroupBy(${safeString(attribute)},$groupedStat)"

  /**
    * String that will be parsed into a multi variate descriptive stat
    *
    * @param attributes attribute name to evaluate
    * @return
    */
  def DescriptiveStats(attributes: Seq[String]): String = s"DescriptiveStats(${attributes.map(safeString).mkString(",")})"

  /**
    * Combines a sequence of stats. This will not modify any of the inputs.
    *
    * @param stats stats to combine
    * @return
    */
  def combine[T <: Stat](stats: Seq[T]): Option[T] = {
    if (stats.length < 2) {
      stats.headOption
    } else {
      // create a new stat so that we don't modify the existing ones
      val summed = stats.head + stats.tail.head
      stats.drop(2).foreach(summed += _)
      Some(summed.asInstanceOf[T])
    }
  }

  // note: adds quotes around the string
  private def safeString(s: String): String = s""""${StringEscapeUtils.escapeJava(s)}""""

  /**
    * Converts a value to a string
    *
    * @param clas class of the input value
    * @param json will the result be used in json? will quote appropriately if so
    * @tparam T type of input
    * @return
    */
  def stringifier[T](clas: Class[T], json: Boolean = false): Any => String = {
    val toString: (Any) => String = if (classOf[Geometry].isAssignableFrom(clas)) {
      (v) => WKTUtils.write(v.asInstanceOf[Geometry])
    } else if (clas == classOf[Date]) {
      (v) => GeoToolsDateFormat.format(toInstant(v.asInstanceOf[Date]))
    } else {
      (v) => v.toString
    }

    // add quotes to json strings if needed
    if (json && !classOf[Number].isAssignableFrom(clas)) {
      (v) => if (v == null) "null" else s""""${toString(v)}""""
    } else {
      (v) => if (v == null) "null" else toString(v)
    }
  }

  /**
    * Converts a string back to a value
    *
    * @param clas class of the stringified value
    * @tparam T type of the value class
    * @return
    */
  def destringifier[T](clas: Class[T]): String => T =
    if (clas == classOf[String]) {
      (s) => if (s == "null") null.asInstanceOf[T] else s.asInstanceOf[T]
    } else if (clas == classOf[Integer]) {
      (s) => if (s == "null") null.asInstanceOf[T] else s.toInt.asInstanceOf[T]
    } else if (clas == classOf[jLong]) {
      (s) => if (s == "null") null.asInstanceOf[T] else s.toLong.asInstanceOf[T]
    } else if (clas == classOf[jFloat]) {
      (s) => if (s == "null") null.asInstanceOf[T] else s.toFloat.asInstanceOf[T]
    } else if (clas == classOf[jDouble]) {
      (s) => if (s == "null") null.asInstanceOf[T] else s.toDouble.asInstanceOf[T]
    } else if (classOf[Geometry].isAssignableFrom(clas)) {
      (s) => if (s == "null") null.asInstanceOf[T] else WKTUtils.read(s).asInstanceOf[T]
    } else if (clas == classOf[Date]) {
      (s) => if (s == "null") null.asInstanceOf[T] else java.util.Date.from(java.time.LocalDateTime.parse(s, GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC)).asInstanceOf[T]
    } else {
      throw new RuntimeException(s"Unexpected class binding for stat attribute: $clas")
    }
}

trait ImmutableStat extends Stat {

  override def observe(sf: SimpleFeature): Unit = fail()

  override def unobserve(sf: SimpleFeature): Unit = fail()

  override def +=(other: S): Unit = fail()

  override def clear(): Unit = fail()

  private def fail(): Unit = throw new RuntimeException("This stat is immutable")
}
