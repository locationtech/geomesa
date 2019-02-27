/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.stats

import java.io.{Closeable, Flushable}
import java.util.Date

import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.filter.visitor.BoundsFilterVisitor
import org.locationtech.geomesa.index.stats.GeoMesaStats.StatUpdater
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.stats.{Histogram, MinMax, Stat}
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.reflect.ClassTag

/**
 * Tracks stats for a schema - spatial/temporal bounds, number of records, etc. Persistence of
 * stats is not part of this trait, as different implementations will likely have different method signatures.
 */
trait GeoMesaStats extends Closeable {

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
    * Gets the bounds for data that will be returned for a query
    *
    * @param sft simple feature type
    * @param filter cql filter
    * @param exact rough estimate, or precise bounds. note: precise bounds will likely be expensive.
    * @return bounds
    */
  def getBounds(sft: SimpleFeatureType, filter: Filter = Filter.INCLUDE, exact: Boolean = false): ReferencedEnvelope = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val filterBounds = BoundsFilterVisitor.visit(filter)
    Option(sft.getGeomField).flatMap(getAttributeBounds[Geometry](sft, _, filter, exact)).map { bounds =>
      val env = bounds.min.getEnvelopeInternal
      env.expandToInclude(bounds.max.getEnvelopeInternal)
      filterBounds.intersection(env)
    }.getOrElse(filterBounds)
  }

  /**
    * Gets the minimum and maximum values for the given attribute
    *
    * @param sft simple feature type
    * @param attribute attribute name to examine
    * @param filter cql filter
    * @param exact rough estimate, or precise values. note: precise values will likely be expensive.
    * @tparam T attribute type - must correspond to attribute binding
    * @return mix/max values and overall cardinality. types will be consistent with the binding of the attribute
    */
  def getAttributeBounds[T](sft: SimpleFeatureType,
                            attribute: String,
                            filter: Filter = Filter.INCLUDE,
                            exact: Boolean = false): Option[MinMax[T]]

  /**
    * Gets existing stats for the given schema
    *
    * @param sft simple feature type
    * @param attributes attribute names to examine, or all attributes
    * @param options stat-specific options, if any
    * @return stats, if any
    */
  def getStats[T <: Stat](sft: SimpleFeatureType,
                          attributes: Seq[String] = Seq.empty,
                          options: Seq[Any] = Seq.empty)(implicit ct: ClassTag[T]): Seq[T]

  /**
    * Executes a query against live data to calculate a given stat
    *
    * @param sft simple feature type
    * @param stats stat string
    * @param filter cql filter
    * @tparam T stat type - must correspond to stat string
    * @return stat
    */
  def runStats[T <: Stat](sft: SimpleFeatureType, stats: String, filter: Filter = Filter.INCLUDE): Seq[T]

  /**
    * Updates the cached stats for the given schema
    *
    * @param sft simple feature type
    */
  def generateStats(sft: SimpleFeatureType): Seq[Stat]

  /**
    * Gets an object to track stats as they are written
    *
    * @param sft simple feature type
    * @return updater
    */
  def statUpdater(sft: SimpleFeatureType): StatUpdater

  /**
    * Deletes any stats associated with the given schema
    *
    * @param sft simple feature type
    */
  def clearStats(sft: SimpleFeatureType): Unit
}

object GeoMesaStats {

  import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

  // date bucket size in milliseconds for the date frequency - one day
  val DateFrequencyPrecision: Int = 1000 * 60 * 60 * 24

  // how many buckets to sort each attribute into
  // max space on disk = 8 bytes * size - we use optimized serialization so likely 1-3 bytes * size
  // buckets up to ~2M values will take 3 bytes or less
  val MaxHistogramSize: Int = 10000 // with ~1B records ~100k records per bin and ~29 kb on disk
  val DefaultHistogramSize: Int = 1000

  val StatClasses = Seq(classOf[Geometry], classOf[String], classOf[Integer],
    classOf[jLong], classOf[jFloat], classOf[jDouble], classOf[Date])

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
      case b if b == classOf[jLong]                   => 0L
      case b if b == classOf[jFloat]                  => 0f
      case b if b == classOf[jDouble]                 => 0d
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
      case c if c == classOf[jLong]               => 1    // size of a 'bin'
      case c if c == classOf[jFloat]              => 1000 // 10 ^ decimal places we'll keep
      case c if c == classOf[jDouble]             => 1000 // 10 ^ decimal places we'll keep
      case c if classOf[Date].isAssignableFrom(c) => 1000 * 60 * 60 // size of a 'bin' - one hour
      case c => throw new NotImplementedError(s"Can't handle binding of type $c")
    }
  }

  // determines if it is possible to run a min/max and histogram on the attribute
  // TODO GEOMESA-1217 support list/maps in stats
  def okForStats(d: AttributeDescriptor): Boolean =
    !d.isMultiValued && StatClasses.exists(_.isAssignableFrom(d.getType.getBinding))

  /**
    * Trait for tracking stats based on simple features
    */
  trait StatUpdater extends Closeable with Flushable {
    def add(sf: SimpleFeature): Unit
    def remove(sf: SimpleFeature): Unit
  }
}
