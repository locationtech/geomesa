/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.accumulo.index

import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.Range
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.joda.time.Weeks
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.index.QueryPlanners.FeatureFunction
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType

class Z3IdxStrategy(val filter: QueryFilter) extends Strategy with Logging with IndexFilterHelpers  {

  import FilterHelper._
  import Z3IdxStrategy._

  val Z3_CURVE = new Z3SFC

  /**
   * Plans the query - strategy implementations need to define this
   */
  override def getQueryPlans(queryPlanner: QueryPlanner, hints: Hints, output: ExplainerOutputType) = {
    val sft = queryPlanner.sft
    val acc = queryPlanner.acc

    val dtgField = sft.getDtgField

    val (geomFilters, temporalFilters) = {
      val (g, t) = filter.primary.partition(isSpatialFilter)
      if (g.isEmpty) {
        // allow for date only queries - if no geom, use whole world
        (Seq(ff.bbox(sft.getGeomField, -180, -90, 180, 90, "EPSG:4326")), t)
      } else {
        (g, t)
      }
    }
    val ecql = filter.secondary

    output(s"Geometry filters: ${filtersToString(geomFilters)}")
    output(s"Temporal filters: ${filtersToString(temporalFilters)}")

    val tweakedGeomFilters = geomFilters.map(updateTopologicalFilters(_, sft))

    output(s"Tweaked geom filters are $tweakedGeomFilters")

    // standardize the two key query arguments:  polygon and date-range
    val geomsToCover = tweakedGeomFilters.flatMap(decomposeToGeometry)

    val collectionToCover: Geometry = geomsToCover match {
      case Nil => null
      case seq: Seq[Geometry] => new GeometryCollection(geomsToCover.toArray, geomsToCover.head.getFactory)
    }

    // since we don't apply a temporal filter, we pass offsetDuring to
    // make sure we exclude the non-inclusive endpoints of a during filter.
    // note that this isn't completely accurate, as we only index down to the second
    val interval = extractInterval(temporalFilters, dtgField, offsetDuring = true)
    val geometryToCover = netGeom(collectionToCover)

    output(s"GeomsToCover: $geometryToCover")
    output(s"Interval:  $interval")

    val fp = FILTERING_ITER_PRIORITY

    val (iterators, kvsToFeatures, colFamily) = if (hints.isBinQuery) {
      val trackId = hints.getBinTrackIdField
      val geom = hints.getBinGeomField
      val dtg = hints.getBinDtgField
      val label = hints.getBinLabelField

      val batchSize = hints.getBinBatchSize
      val sort = hints.isBinSorting

      // if possible, use the pre-computed values
      // can't use if there are non-st filters or if custom fields are requested
      val (iters, cf) =
        if (ecql.isEmpty && BinAggregatingIterator.canUsePrecomputedBins(sft, trackId, geom, dtg, label)) {
          (Seq(BinAggregatingIterator.configurePrecomputed(sft, ecql, batchSize, sort, fp)), Z3Table.BIN_CF)
        } else {
          val binDtg = dtg.getOrElse(dtgField.get) // dtgField is always defined if we're using z3
          val binGeom = geom.getOrElse(sft.getGeomField)
          val iter = BinAggregatingIterator.configureDynamic(sft, ecql, trackId, binGeom, binDtg, label,
            batchSize, sort, fp)
          (Seq(iter), Z3Table.FULL_CF)
        }
      (iters, BinAggregatingIterator.kvsToFeatures(), cf)
    } else if (hints.isDensityQuery) {
      val envelope = hints.getDensityEnvelope.get
      val (width, height) = hints.getDensityBounds.get
      val weight = hints.getDensityWeight
      val iter = Z3DensityIterator.configure(sft, ecql, envelope, width, height, weight, fp)
      (Seq(iter), Z3DensityIterator.kvsToFeatures(), Z3Table.FULL_CF)
    } else {
      val transforms = for {
        tdef <- hints.getTransformDefinition
        tsft <- hints.getTransformSchema
      } yield { (tdef, tsft) }
      output(s"Transforms: $transforms")

      val iters = (ecql, transforms) match {
        case (None, None) => Seq.empty
        case _ => Seq(KryoLazyFilterTransformIterator.configure(sft, ecql, transforms, fp))
      }
      (iters, Z3Table.adaptZ3KryoIterator(hints.getReturnSft), Z3Table.FULL_CF)
    }

    val z3table = acc.getTableName(sft.getTypeName, Z3Table)
    val numThreads = acc.getSuggestedThreads(sft.getTypeName, Z3Table)

    // setup Z3 iterator
    val env = geometryToCover.getEnvelopeInternal
    val (lx, ly, ux, uy) = (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
    val epochWeekStart = Weeks.weeksBetween(Z3Table.EPOCH, interval.getStart)
    val epochWeekEnd = Weeks.weeksBetween(Z3Table.EPOCH, interval.getEnd)
    val weeks = scala.Range.inclusive(epochWeekStart.getWeeks, epochWeekEnd.getWeeks)
    val lt = Z3Table.secondsInCurrentWeek(interval.getStart, epochWeekStart)
    val ut = Z3Table.secondsInCurrentWeek(interval.getEnd, epochWeekEnd)
    if (weeks.length == 1) {
      Seq(queryPlanForPrefix(weeks.head, lt ,ut, lx, ly, ux, uy,
        z3table, kvsToFeatures, iterators, colFamily, numThreads, contained = false))
    } else {
      val oneWeekInSeconds = Weeks.ONE.toStandardSeconds.getSeconds
      val head +: xs :+ last = weeks.toList
      val middleQPs = xs.map { w =>
        queryPlanForPrefix(w, 0, oneWeekInSeconds, lx, ly, ux, uy,
          z3table, kvsToFeatures, iterators, colFamily, numThreads, contained = true)
      }
      val startQP = queryPlanForPrefix(head, lt, oneWeekInSeconds, lx, ly, ux, uy,
        z3table, kvsToFeatures, iterators, colFamily, numThreads, contained = false)
      val endQP = queryPlanForPrefix(last, 0, ut, lx, ly, ux, uy,
        z3table, kvsToFeatures, iterators, colFamily, numThreads, contained = false)
      Seq(startQP, endQP) ++ middleQPs
    }
  }

  def queryPlanForPrefix(week: Int, lt: Long, ut: Long,
                         lx: Double, ly: Double, ux: Double, uy: Double,
                         table: String,
                         kvsToFeatures: FeatureFunction,
                         is: Seq[IteratorSetting],
                         colFamily: Text,
                         numThreads: Int,
                         contained: Boolean = true) = {
    val epochWeekStart = Weeks.weeks(week)
    val prefix = Shorts.toByteArray(epochWeekStart.getWeeks.toShort)

    val z3ranges = Z3_CURVE.ranges(lx, ly, ux, uy, lt, ut, 8)

    val accRanges = z3ranges.map { case (s, e) =>
      val startRowBytes = Bytes.concat(prefix, Longs.toByteArray(s))
      val endRowBytes = Bytes.concat(prefix, Longs.toByteArray(e))
      val start = new Text(startRowBytes)
      val end = Range.followingPrefix(new Text(endRowBytes))
      new Range(start, true, end, false)
    }

    val iter = Z3Iterator.configure(Z3_CURVE.index(lx, ly, lt), Z3_CURVE.index(ux, uy, ut), Z3_ITER_PRIORITY)

    val iters = Seq(iter) ++ is
    BatchScanPlan(table, accRanges, iters, Seq(colFamily), kvsToFeatures, numThreads, hasDuplicates = false)
  }
}

object Z3IdxStrategy extends StrategyProvider {

  val Z3_ITER_PRIORITY = 21
  val FILTERING_ITER_PRIORITY = 25

  /**
   * Gets the estimated cost of running the query. Currently, cost is hard-coded to sort between
   * strategies the way we want. Z3 should be more than id lookups (at 1), high-cardinality attributes (at 1)
   * and less than STidx (at 400) and unknown cardinality attributes (at 999).
   *
   * Eventually cost will be computed based on dynamic metadata and the query.
   */
  override def getCost(filter: QueryFilter, sft: SimpleFeatureType, hints: StrategyHints) =
    if (filter.primary.length > 1) 200 else 400
}
