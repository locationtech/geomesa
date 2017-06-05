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
import com.vividsolutions.jts.geom.Geometry
import org.geotools.factory.Hints
import org.joda.time.DateTime
import org.locationtech.geomesa.curve.{BinnedTime, XZ3SFC}
import org.locationtech.geomesa.filter.{filterToString, _}
import org.locationtech.geomesa.index.api.{FilterStrategy, GeoMesaFeatureIndex, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.strategies.SpatioTemporalFilterStrategy
import org.locationtech.geomesa.index.utils.{ByteArrays, Explainer, SplitArrays}
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, _}
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

trait XZ3Index[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R] extends GeoMesaFeatureIndex[DS, F, W]
    with IndexAdapter[DS, F, W, R] with SpatioTemporalFilterStrategy[DS, F, W] with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = "xz3"

  override def supports(sft: SimpleFeatureType): Boolean = XZ3Index.supports(sft)

  override def writer(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sharing = sft.getTableSharingBytes
    val shards = SplitArrays(sft)
    val toIndexKey = XZ3Index.toIndexKey(sft)
    (wf) => Seq(createInsert(getRowKey(sharing, shards, toIndexKey, wf), wf))
  }

  override def remover(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sharing = sft.getTableSharingBytes
    val shards = SplitArrays(sft)
    val toIndexKey = XZ3Index.toIndexKey(sft)
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
      val ranges = filter.primary match {
        case None =>
          filter.secondary.foreach { f =>
            logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
          }
          Seq(rangePrefix(sharing))

        case Some(f) =>
          val splits = SplitArrays(sft)
          val prefixes = if (sharing.length == 0) { splits } else { splits.map(Bytes.concat(sharing, _)) }
          XZ3Index.getRanges(sft, f, explain).flatMap { case (s, e) =>
            prefixes.map(p => range(Bytes.concat(p, s), Bytes.concat(p, e)))
          }.toSeq
      }

      scanPlan(sft, ds, filter, hints, ranges, filter.filter)
    } finally {
      XZ3Index.clearProcessingValues()
    }
  }
}

object XZ3Index extends IndexKeySpace[XZ3ProcessingValues] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val indexKeyLength: Int = 10

  override def supports(sft: SimpleFeatureType): Boolean = sft.getDtgField.isDefined && sft.nonPoints

  override def toIndexKey(sft: SimpleFeatureType): (SimpleFeature) => Array[Byte] = {
    val sfc = XZ3SFC(sft.getXZPrecision, sft.getZ3Interval)
    val geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new IllegalStateException("XZ3 index requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)

    (feature) => {
      val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]
      if (geom == null) {
        throw new IllegalArgumentException(s"Null geometry in feature ${feature.getID}")
      }
      val envelope = geom.getEnvelopeInternal
      // TODO support date intervals (remember to remove disjoint data check in getRanges)
      val dtg = feature.getAttribute(dtgIndex).asInstanceOf[Date]
      val time = if (dtg == null) { 0L } else { dtg.getTime }
      val BinnedTime(b, t) = timeToIndex(time)
      val xz = try { sfc.index(envelope.getMinX, envelope.getMinY, t, envelope.getMaxX, envelope.getMaxY, t) } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid xz value from geometry/time: $geom,$dtg", e)
      }
      ByteArrays.toBytes(b, xz)
    }
  }

  override def getRanges(sft: SimpleFeatureType,
                         filter: Filter,
                         explain: Explainer): Iterator[(Array[Byte], Array[Byte])] = {
    import org.locationtech.geomesa.filter.FilterHelper._

    // note: z3 requires a date field
    val dtgField = sft.getDtgField.getOrElse {
      throw new RuntimeException("Trying to execute an xz3 query but the schema does not have a date")
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

    // disjoint geometries are ok since they could still intersect a polygon
    if (intervals.disjoint) {
      explain("Disjoint dates extracted, short-circuiting to empty query")
      return Iterator.empty
    }

    val sfc = XZ3SFC(sft.getXZPrecision, sft.getZ3Interval)
    val minTime = 0.0
    val maxTime = BinnedTime.maxOffset(sft.getZ3Interval).toDouble
    val wholePeriod = (minTime, maxTime)

    val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)

    // compute our ranges based on the coarse bounds for our query

    val xy = geometries.values.map(GeometryUtils.bounds)

    // calculate map of weeks to time intervals in that week
    val timesByBin = scala.collection.mutable.Map.empty[Short, (Double, Double)]
    val dateToIndex = BinnedTime.dateToBinnedTime(sft.getZ3Interval)

    def updateTime(week: Short, lt: Double, ut: Double): Unit = {
      val times = timesByBin.get(week) match {
        case None => (lt, ut)
        case Some((min, max)) => (math.min(min, lt), math.max(max, ut))
      }
      timesByBin(week) = times
    }

    // note: intervals shouldn't have any overlaps
    intervals.foreach { interval =>
      val BinnedTime(lb, lt) = dateToIndex(interval._1)
      val BinnedTime(ub, ut) = dateToIndex(interval._2)
      if (lb == ub) {
        updateTime(lb, lt, ut)
      } else {
        updateTime(lb, lt, maxTime)
        updateTime(ub, minTime, ut)
        Range.inclusive(lb + 1, ub - 1).foreach(b => timesByBin(b.toShort) = wholePeriod)
      }
    }

    // make our underlying index values available to other classes in the pipeline for processing
    processingValues.set(XZ3ProcessingValues(sfc, geometries, xy, intervals, timesByBin.toMap))

    def toZRanges(t: (Double, Double)): Seq[IndexRange] =
      sfc.ranges(xy.map { case (xmin, ymin, xmax, ymax) => (xmin, ymin, t._1, xmax, ymax, t._2) }, rangeTarget)

    lazy val wholePeriodRanges = toZRanges(wholePeriod)

    timesByBin.iterator.flatMap { case (bin, times) =>
      val b = Shorts.toByteArray(bin)
      val zs = if (times.eq(wholePeriod)) { wholePeriodRanges } else { toZRanges(times) }
      zs.map(r => (ByteArrays.toBytes(b, r.lower), ByteArrays.toBytesFollowingPrefix(b, r.upper)))
    }
  }
}

case class XZ3ProcessingValues(sfc: XZ3SFC,
                               geometries: FilterValues[Geometry],
                               spatialBounds: Seq[ (Double, Double, Double, Double)],
                               intervals: FilterValues[(DateTime, DateTime)],
                               temporalBounds: Map[Short, (Double, Double)])