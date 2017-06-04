/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import java.nio.charset.StandardCharsets
import java.util.Date

import com.google.common.primitives.{Bytes, Shorts}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.geotools.factory.Hints
import org.joda.time.DateTime
import org.locationtech.geomesa.curve.{BinnedTime, Z3SFC}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.strategies.SpatioTemporalFilterStrategy
import org.locationtech.geomesa.index.utils.{ByteArrays, Explainer, SplitArrays}
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, _}
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

trait Z3Index[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R] extends GeoMesaFeatureIndex[DS, F, W]
    with IndexAdapter[DS, F, W, R] with SpatioTemporalFilterStrategy[DS, F, W] with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = "z3"

  override def supports(sft: SimpleFeatureType): Boolean = Z3Index.supports(sft)

  override def writer(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sharing = sft.getTableSharingBytes
    val shards = SplitArrays(sft)
    val toIndexKey = Z3Index.toIndexKey(sft)
    (wf) => Seq(createInsert(getRowKey(sharing, shards, toIndexKey, wf), wf))
  }

  override def remover(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sharing = sft.getTableSharingBytes
    val shards = SplitArrays(sft)
    val toIndexKey = Z3Index.toIndexKey(sft)
    (wf) => Seq(createDelete(getRowKey(sharing, shards, toIndexKey, wf), wf))
  }

  private def getRowKey(sharing: Array[Byte],
                        shards: IndexedSeq[Array[Byte]],
                        toIndexKey: (SimpleFeature) => Array[Byte],
                        wrapper: F): Array[Byte] = {
    val split = shards(wrapper.idHash % shards.length)
    val binAndZ = toIndexKey(wrapper.feature)
    Bytes.concat(sharing, split, binAndZ, wrapper.idBytes)
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int) => String = {
    val start = if (sft.isTableSharing) { 12 } else { 11 } // table sharing + shard + 2 byte short + 8 byte long
    (row, offset, length) => new String(row, offset + start, length - start, StandardCharsets.UTF_8)
  }

  override def getSplits(sft: SimpleFeatureType): Seq[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val splits = SplitArrays(sft).drop(1) // drop the first so we don't get an empty tablet
    if (sft.isTableSharing) {
      val sharing = sft.getTableSharingBytes
      splits.map(s => Bytes.concat(sharing, s))
    } else {
      splits
    }
  }

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: DS,
                            filter: FilterStrategy[DS, F, W],
                            hints: Hints,
                            explain: Explainer): QueryPlan[DS, F, W] = {
    val sharing = sft.getTableSharingBytes

    try {
      // compute our accumulo ranges based on the coarse bounds for our query
      val ranges = filter.primary match {
        case None =>
          filter.secondary.foreach { f =>
            logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
          }
          Seq(rangePrefix(sharing))

        case Some(f) =>
          val splits = SplitArrays(sft)
          val prefixes = if (sharing.length == 0) { splits } else { splits.map(Bytes.concat(sharing, _)) }
          Z3Index.getRanges(sft, f, explain).flatMap { case (s, e) =>
            prefixes.map(p => range(Bytes.concat(p, s), Bytes.concat(p, e)))
          }.toSeq
      }

      val looseBBox = Option(hints.get(LOOSE_BBOX)).map(Boolean.unbox).getOrElse(ds.config.looseBBox)

      // if the user has requested strict bounding boxes, we apply the full filter
      // if this is a non-point geometry type, the index is coarse-grained, so we apply the full filter
      // if the spatial predicate is rectangular (e.g. a bbox), the index is fine enough that we
      // don't need to apply the filter on top of it. this may cause some minor errors at extremely
      // fine resolutions, but the performance is worth it
      // if we have a complicated geometry predicate, we need to pass it through to be evaluated
      lazy val simpleGeoms =
        Z3Index.currentProcessingValues.toSeq.flatMap(_.geometries.values).forall(GeometryUtils.isRectangular)

      val ecql = if (looseBBox && simpleGeoms) { filter.secondary } else { filter.filter }

      scanPlan(sft, ds, filter, hints, ranges, ecql)
    } finally {
      // ensure we clear our indexed values
      Z3Index.clearProcessingValues()
    }
  }
}

object Z3Index extends IndexKeySpace[Z3ProcessingValues] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val indexKeyLength: Int = 10

  override def supports(sft: SimpleFeatureType): Boolean = sft.getDtgField.isDefined && sft.isPoints

  override def toIndexKey(sft: SimpleFeatureType): (SimpleFeature) => Array[Byte] = {
    val sfc = Z3SFC(sft.getZ3Interval)
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    val geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new IllegalStateException("Z3 index requires a valid date"))

    (feature) => {
      val geom = feature.getAttribute(geomIndex).asInstanceOf[Point]
      if (geom == null) {
        throw new IllegalArgumentException(s"Null geometry in feature ${feature.getID}")
      }
      val dtg = feature.getAttribute(dtgIndex).asInstanceOf[Date]
      val time = if (dtg == null) { 0 } else { dtg.getTime }
      val BinnedTime(b, t) = timeToIndex(time)
      val z = try { sfc.index(geom.getX, geom.getY, t).z } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid z value from geometry/time: $geom,$dtg", e)
      }
      ByteArrays.toBytes(b, z)
    }
  }

  override def getRanges(sft: SimpleFeatureType,
                         filter: Filter,
                         explain: Explainer): Iterator[(Array[Byte], Array[Byte])] = {

    import org.locationtech.geomesa.filter.FilterHelper._

    val dtgField = sft.getDtgField.getOrElse {
      throw new RuntimeException("Trying to execute a z3 query but the schema does not have a date")
    }

    // standardize the two key query arguments:  polygon and date-range

    val geometries: FilterValues[Geometry] = {
      val extracted = extractGeometries(filter, sft.getGeomField, sft.isPoints)
      if (extracted.nonEmpty) { extracted } else { FilterValues(Seq(WholeWorldPolygon)) }
    }

    // since we don't apply a temporal filter, we pass handleExclusiveBounds to
    // make sure we exclude the non-inclusive endpoints of a during filter.
    // note that this isn't completely accurate, as we only index down to the second
    val intervals = extractIntervals(filter, dtgField, handleExclusiveBounds = true)

    explain(s"Geometries: $geometries")
    explain(s"Intervals: $intervals")

    if (geometries.disjoint || intervals.disjoint) {
      explain("Disjoint geometries or dates extracted, short-circuiting to empty query")
      return Iterator.empty
    }

    val sfc = Z3SFC(sft.getZ3Interval)
    val minTime = sfc.time.min.toLong
    val maxTime = sfc.time.max.toLong
    val wholePeriod = Seq((minTime, maxTime))

    // compute our accumulo ranges based on the coarse bounds for our query
    val xy = geometries.values.map(GeometryUtils.bounds)

    // calculate map of weeks to time intervals in that week
    val timesByBin = scala.collection.mutable.Map.empty[Short, Seq[(Long, Long)]].withDefaultValue(Seq.empty)
    val dateToIndex = BinnedTime.dateToBinnedTime(sft.getZ3Interval)
    // note: intervals shouldn't have any overlaps
    intervals.foreach { interval =>
      val BinnedTime(lb, lt) = dateToIndex(interval._1)
      val BinnedTime(ub, ut) = dateToIndex(interval._2)
      if (lb == ub) {
        timesByBin(lb) ++= Seq((lt, ut))
      } else {
        timesByBin(lb) ++= Seq((lt, maxTime))
        timesByBin(ub) ++= Seq((minTime, ut))
        Range.inclusive(lb + 1, ub - 1).foreach(b => timesByBin(b.toShort) = wholePeriod)
      }
    }

    // make our underlying index values available to other classes in the pipeline for processing
    processingValues.set(Z3ProcessingValues(sfc, geometries, xy, intervals, timesByBin.toMap))

    val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)

    def toZRanges(t: Seq[(Long, Long)]): Seq[IndexRange] = sfc.ranges(xy, t, 64, rangeTarget)

    lazy val wholePeriodRanges = toZRanges(wholePeriod)

    timesByBin.iterator.flatMap { case (bin, times) =>
      val b = Shorts.toByteArray(bin)
      val zs = if (times.eq(wholePeriod)) { wholePeriodRanges } else { toZRanges(times) }
      zs.map(range => (ByteArrays.toBytes(b, range.lower), ByteArrays.toBytesFollowingPrefix(b, range.upper)))
    }
  }
}

case class Z3ProcessingValues(sfc: Z3SFC,
                              geometries: FilterValues[Geometry],
                              spatialBounds: Seq[ (Double, Double, Double, Double)],
                              intervals: FilterValues[(DateTime, DateTime)],
                              temporalBounds: Map[Short, Seq[(Long, Long)]])