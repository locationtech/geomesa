/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.stats

import java.io.{Closeable, Flushable}
import java.util.Date

import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.filter.visitor.BoundsFilterVisitor
import org.locationtech.geomesa.index.stats.GeoMesaStats.GeoMesaStatWriter
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.stats._
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
 * Tracks stats for a schema - spatial/temporal bounds, number of records, etc. Persistence of
 * stats is not part of this trait, as different implementations will likely have different method signatures.
 */
trait GeoMesaStats extends Closeable {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  /**
    * Gets a writer for updating stats
    *
    * @return
    */
  def writer: GeoMesaStatWriter

  /**
    * Gets the number of features that will be returned for a query. May return -1 if exact is false
    * and estimate is unavailable.
    *
    * @param sft simple feature type
    * @param filter cql filter
    * @param exact rough estimate, or precise count. note: precise count will likely be expensive.
    * @return count of features, if available - will always be Some if exact == true
    */
  def getCount(sft: SimpleFeatureType, filter: Filter = Filter.INCLUDE, exact: Boolean = false): Option[Long]

  /**
    * Get the bounds for data that will be returned for a query
    *
    * @param sft simple feature type
    * @param filter cql filter
    * @param exact rough estimate, or precise bounds. note: precise bounds will likely be expensive.
    * @return bounds
    */
  def getBounds(
      sft: SimpleFeatureType,
      filter: Filter = Filter.INCLUDE,
      exact: Boolean = false): ReferencedEnvelope = {
    val filterBounds = BoundsFilterVisitor.visit(filter)
    Option(sft.getGeomField).flatMap(getMinMax[Geometry](sft, _, filter, exact)) match {
      case None => filterBounds
      case Some(bounds) =>
        val env = bounds.min.getEnvelopeInternal
        env.expandToInclude(bounds.max.getEnvelopeInternal)
        filterBounds.intersection(env)
    }
  }

  /**
    * Get the minimum and maximum values for the given attribute
    *
    * @param sft simple feature type
    * @param attribute attribute name to examine
    * @param filter cql filter
    * @param exact rough estimate, or precise values. note: precise values will likely be expensive.
    * @tparam T attribute type - must correspond to attribute binding
    * @return mix/max values and overall cardinality. types will be consistent with the binding of the attribute
    */
  def getMinMax[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter = Filter.INCLUDE,
      exact: Boolean = false): Option[MinMax[T]]

  /**
    * Get an enumeration stat
    *
    * @param sft simple feature type
    * @param attribute attribute name to query
    * @param filter cql filter
    * @param exact rough estimates, or precise values. note: precise values will likely be expensive.
    * @tparam T attribute type - must correspond to attribute binding
    * @return
    */
  def getEnumeration[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter = Filter.INCLUDE,
      exact: Boolean = false): Option[EnumerationStat[T]]

  /**
    * Get a frequency stat
    *
    * @param sft simple feature type
    * @param attribute attribute name to query
    * @param precision precision of the estimate - @see org.locationtech.geomesa.utils.stats.Frequency
    * @param filter cql filter
    * @param exact rough estimates, or precise values. note: precise values will likely be expensive.
    * @tparam T attribute type - must correspond to attribute binding
    * @return
    */
  def getFrequency[T](
      sft: SimpleFeatureType,
      attribute: String,
      precision: Int,
      filter: Filter = Filter.INCLUDE,
      exact: Boolean = false): Option[Frequency[T]]

  /**
    * Get a top k stat
    *
    * @param sft simple feature type
    * @param attribute attribute name to query
    * @param filter cql filter
    * @param exact rough estimates, or precise values. note: precise values will likely be expensive.
    * @tparam T attribute type - must correspond to attribute binding
    * @return
    */
  def getTopK[T](
      sft: SimpleFeatureType,
      attribute: String,
      filter: Filter = Filter.INCLUDE,
      exact: Boolean = false): Option[TopK[T]]

  /**
    * Get a histogram stat
    *
    * @param sft simple feature type
    * @param attribute attribute name to query
    * @param bins number of buckets used to group values
    * @param min minimum value used to create the initial histogram buckets
    * @param max maximum value used to create the initial histogram buckets
    * @param filter cql filter
    * @param exact rough estimates, or precise values. note: precise values will likely be expensive.
    * @tparam T attribute type - must correspond to attribute binding
    * @return
    */
  def getHistogram[T](
      sft: SimpleFeatureType,
      attribute: String,
      bins: Int,
      min: T,
      max: T,
      filter: Filter = Filter.INCLUDE,
      exact: Boolean = false): Option[Histogram[T]]

  /**
    * Get a Z3 histogram stat, where values are grouped based on combined geometry + date
    *
    * @param sft simple feature type
    * @param geom geometry attribute to query
    * @param dtg date attribute to query
    * @param period time period used to calculate bins for each value
    * @param bins number of buckets used to group values
    * @param filter cql filter
    * @param exact rough estimates, or precise values. note: precise values will likely be expensive.
    * @return
    */
  def getZ3Histogram(
      sft: SimpleFeatureType,
      geom: String,
      dtg: String,
      period: TimePeriod,
      bins: Int,
      filter: Filter = Filter.INCLUDE,
      exact: Boolean = false): Option[Z3Histogram]

  /**
    * Gets arbitrary stats for multiple queries
    *
    * @param sft simple feature type
    * @param queries stats strings
    * @param filter cql filter
    * @param exact rough estimate, or precise values. note: precise values will likely be expensive.
    * @tparam T type bounds, must match stat query strings
    * @return
    */
  def getSeqStat[T <: Stat](
      sft: SimpleFeatureType,
      queries: Seq[String],
      filter: Filter = Filter.INCLUDE,
      exact: Boolean = false): Seq[T] = {
    if (queries.isEmpty) {
      Seq.empty
    } else if (queries.lengthCompare(1) == 0) {
      getStat(sft, queries.head, filter, exact).toSeq
    } else {
      getStat[SeqStat](sft, Stat.SeqStat(queries), filter, exact) match {
        case None    => Seq.empty
        case Some(s) => s.stats.asInstanceOf[Seq[T]]
      }
    }
  }

  /**
    * Get arbitrary stats
    *
    * @param sft simple feature type
    * @param query stats string
    * @param filter cql filter
    * @param exact rough estimate, or precise values. note: precise values will likely be expensive.
    * @tparam T type bounds, must match stat query strings
    * @return stats, if any
    */
  def getStat[T <: Stat](
      sft: SimpleFeatureType,
      query: String,
      filter: Filter = Filter.INCLUDE,
      exact: Boolean = false): Option[T]
}

object GeoMesaStats {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  // date bucket size in milliseconds for the date frequency - one day
  val DateFrequencyPrecision: Int = 1000 * 60 * 60 * 24

  // how many buckets to sort each attribute into
  // max space on disk = 8 bytes * size - we use optimized serialization so likely 1-3 bytes * size
  // buckets up to ~2M values will take 3 bytes or less
  val MaxHistogramSize: Int = 10000 // with ~1B records ~100k records per bin and ~29 kb on disk
  val DefaultHistogramSize: Int = 1000

  val StatClasses: Seq[Class[_ <: AnyRef]] =
    Seq(classOf[Geometry], classOf[String], classOf[Integer], classOf[java.lang.Long],
      classOf[java.lang.Float], classOf[java.lang.Double], classOf[Date])

  /**
    * Get the default bounds for a range histogram
    *
    * @param binding class type
    * @tparam T class type
    * @return bounds
    */
  def defaultBounds[T](binding: Class[T]): (T, T) = {
    val default = binding match {
      case b if b == classOf[String]                  => ""
      case b if b == classOf[Integer]                 => 0
      case b if b == classOf[java.lang.Long]          => 0L
      case b if b == classOf[java.lang.Float]         => 0f
      case b if b == classOf[java.lang.Double]        => 0d
      case b if classOf[Date].isAssignableFrom(b)     => new Date()
      case b if classOf[Geometry].isAssignableFrom(b) => GeometryUtils.zeroPoint
      case _ => throw new NotImplementedError(s"Can't handle binding of type $binding")
    }
    Histogram.buffer(default.asInstanceOf[T])
  }

  /**
    * Gets the default precision for a frequency stat
    *
    * @param binding class type
    * @return precision
    */
  def defaultPrecision(binding: Class[_]): Int = {
    binding match {
      case c if c == classOf[String]              => 20   // number of characters we will compare
      case c if c == classOf[Integer]             => 1    // size of a 'bin'
      case c if c == classOf[java.lang.Long]      => 1    // size of a 'bin'
      case c if c == classOf[java.lang.Float]     => 1000 // 10 ^ decimal places we'll keep
      case c if c == classOf[java.lang.Double]    => 1000 // 10 ^ decimal places we'll keep
      case c if classOf[Date].isAssignableFrom(c) => 1000 * 60 * 60 // size of a 'bin' - one hour
      case c => throw new NotImplementedError(s"Can't handle binding of type $c")
    }
  }

  // determines if it is possible to run a min/max and histogram on the attribute
  // TODO GEOMESA-1217 support list/maps in stats
  def okForStats(d: AttributeDescriptor): Boolean =
    !d.isMultiValued && StatClasses.exists(_.isAssignableFrom(d.getType.getBinding))

  /**
    * Trait for writing/updating stats
    */
  trait GeoMesaStatWriter {

    /**
      * Updates the persisted stats for the given schema
      *
      * @param sft simple feature type
      */
    def analyze(sft: SimpleFeatureType): Seq[Stat]

    /**
      * Gets an object to track stats as they are written
      *
      * @param sft simple feature type
      * @return updater
      */
    def updater(sft: SimpleFeatureType): StatUpdater

    /**
      * Renames a schema and/or attributes
      *
      * @param sft simple feature type
      * @param previous old feature type to migrate
      */
    def rename(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit

    /**
      * Deletes any stats associated with the given schema
      *
      * @param sft simple feature type
      */
    def clear(sft: SimpleFeatureType): Unit
  }

  /**
    * Trait for tracking stats based on simple features
    */
  trait StatUpdater extends Closeable with Flushable {
    def add(sf: SimpleFeature): Unit
    def remove(sf: SimpleFeature): Unit
  }
}
