/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.index.index

import java.nio.charset.StandardCharsets
import java.util.Date

import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.geotools.factory.Hints
import org.locationtech.geomesa.curve.{BinnedTime, XZ3SFC}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, QueryPlan, WrappedFeature}
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.strategies.SpatioTemporalFilterStrategy
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, _}
import org.opengis.feature.simple.SimpleFeatureType

trait XZ3Index[DS <: GeoMesaDataStore[DS, F, W, Q], F <: WrappedFeature, W, Q, R] extends GeoMesaFeatureIndex[DS, F, W, Q]
    with IndexAdapter[DS, F, W, Q, R] with SpatioTemporalFilterStrategy[DS, F, W, Q] with LazyLogging {

  import IndexAdapter.{DefaultNumSplits, DefaultSplitArrays}
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name: String = "xz3"

  override def supports(sft: SimpleFeatureType): Boolean = sft.getDtgField.isDefined && sft.nonPoints

  override def writer(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sfc = XZ3SFC(sft.getXZPrecision, sft.getZ3Interval)
    val sharing = sft.getTableSharingBytes
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new RuntimeException("XZ3 writer requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)

    (wf) => Seq(createInsert(getRowKey(sfc, sharing, dtgIndex, timeToIndex, wf), wf))
  }

  override def remover(sft: SimpleFeatureType, ds: DS): (F) => Seq[W] = {
    val sfc = XZ3SFC(sft.getXZPrecision, sft.getZ3Interval)
    val sharing = sft.getTableSharingBytes
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new RuntimeException("XZ3 writer requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)

    (wf) => Seq(createDelete(getRowKey(sfc, sharing, dtgIndex, timeToIndex, wf), wf))
  }

  private def getRowKey(sfc: XZ3SFC,
                        sharing: Array[Byte],
                        dtgIndex: Int,
                        timeToIndex: (Long) => BinnedTime,
                        wrapper: F): Array[Byte] = {
    val split = DefaultSplitArrays(wrapper.idHash % DefaultNumSplits)
    val envelope = wrapper.feature.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal
    // TODO support date intervals
    val dtg = wrapper.feature.getAttribute(dtgIndex).asInstanceOf[Date]
    val time = if (dtg == null) 0 else dtg.getTime
    val BinnedTime(b, t) = timeToIndex(time)
    val xz = sfc.index(envelope.getMinX, envelope.getMinY, t, envelope.getMaxX, envelope.getMaxY, t)
    val id = wrapper.feature.getID.getBytes(StandardCharsets.UTF_8)
    Bytes.concat(sharing, split, Shorts.toByteArray(b), Longs.toByteArray(xz), id)
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int) => String = {
    val start = if (sft.isTableSharing) { 12 } else { 11 } // table sharing + shard + 2 byte short + 8 byte long
    (row, offset, length) => new String(row, offset + start, length - start, StandardCharsets.UTF_8)
  }

  override def getSplits(sft: SimpleFeatureType): Seq[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val splits = DefaultSplitArrays.drop(1) // drop the first so we don't get an empty tablet
    if (sft.isTableSharing) {
      val sharing = sft.getTableSharingBytes
      splits.map(s => Bytes.concat(sharing, s))
    } else {
      splits
    }
  }

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: DS,
                            filter: TypedFilterStrategy,
                            hints: Hints,
                            explain: Explainer): QueryPlan[DS, F, W, Q] = {
    import org.locationtech.geomesa.filter.FilterHelper._

    // note: z3 requires a date field
    val dtgField = sft.getDtgField.getOrElse {
      throw new RuntimeException("Trying to execute a z3 query but the schema does not have a date")
    }

    if (filter.primary.isEmpty) {
      filter.secondary.foreach { f =>
        logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
      }
    }

    // standardize the two key query arguments:  polygon and date-range

    val geometries = filter.primary.map(extractGeometries(_, sft.getGeomField, sft.isPoints))
        .filter(_.nonEmpty).getOrElse(Seq(WholeWorldPolygon))

    // since we don't apply a temporal filter, we pass handleExclusiveBounds to
    // make sure we exclude the non-inclusive endpoints of a during filter.
    // note that this isn't completely accurate, as we only index down to the second
    val intervals = filter.primary.map(extractIntervals(_, dtgField, handleExclusiveBounds = true)).getOrElse(Seq.empty)

    explain(s"Geometries: $geometries")
    explain(s"Intervals: $intervals")

    if (intervals == DisjointInterval) {
      explain("Disjoint dates extracted, short-circuiting to empty query")
      return scanPlan(sft, ds, filter, hints, Seq.empty, None)
    }

    val sfc = XZ3SFC(sft.getXZPrecision, sft.getZ3Interval)
    val minTime = 0.0
    val maxTime = BinnedTime.maxOffset(sft.getZ3Interval).toDouble
    val wholePeriod = (minTime, maxTime)
    val sharing = sft.getTableSharingBytes

    // compute our accumulo ranges based on the coarse bounds for our query
    val ranges = if (filter.primary.isEmpty) { Seq(rangePrefix(sharing)) } else {
      val xy = geometries.map(GeometryUtils.bounds)

      // calculate map of weeks to time intervals in that week
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

      val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)
      def toZRanges(t: (Double, Double)): Seq[(Array[Byte], Array[Byte])] = {
        val query = xy.map { case (xmin, ymin, xmax, ymax) => (xmin, ymin, t._1, xmax, ymax, t._2) }
        sfc.ranges(query, rangeTarget).map(r => (Longs.toByteArray(r.lower), Longs.toByteArray(r.upper)))
      }

      lazy val wholePeriodRanges = toZRanges(wholePeriod)

      val ranges = timesByBin.flatMap { case (b, times) =>
        import com.google.common.primitives.Bytes.concat

        val zs = if (times.eq(wholePeriod)) wholePeriodRanges else toZRanges(times)
        val binBytes = Shorts.toByteArray(b)
        val prefixes = DefaultSplitArrays.map(concat(sharing, _, binBytes))
        prefixes.flatMap { prefix =>
          zs.map { case (lo, hi) =>
            range(concat(prefix, lo), IndexAdapter.rowFollowingRow(concat(prefix, hi)))
          }
        }
      }

      ranges.toSeq
    }

    scanPlan(sft, ds, filter, hints, ranges, filter.filter)
  }
}
