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
import org.locationtech.geomesa.accumulo.index
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.index.QueryPlanners.FeatureFunction
import org.locationtech.geomesa.accumulo.iterators.{BinAggregatingIterator, Z3DensityIterator, Z3Iterator}
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.iterators.{KryoLazyFilterTransformIterator, LazyFilterTransformIterator}
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

    val dtgField = getDtgFieldName(sft)

    val (geomFilters, temporalFilters) = filter.primary.partition(isSpatialFilter)
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

    val interval = netInterval(extractTemporal(dtgField)(temporalFilters))
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
        tdef <- index.getTransformDefinition(hints)
        tsft <- index.getTransformSchema(hints)
      } yield { (tdef, tsft) }
      output(s"Transforms: $transforms")

      val iters = (ecql, transforms) match {
        case (None, None) => Seq.empty
        case _ =>
          Seq(LazyFilterTransformIterator.configure[KryoLazyFilterTransformIterator](sft, ecql, transforms, fp))
      }
      (iters, Z3Table.adaptZ3KryoIterator(hints.getReturnSft), Z3Table.FULL_CF)
    }

    val z3table = acc.getZ3Table(sft)
    val numThreads = acc.getSuggestedZ3Threads(sft)

    // setup Z3 iterator
    val env = geometryToCover.getEnvelopeInternal
    val (lx, ly, ux, uy) = (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)

    def qp(week: Int, lt: Long, ut: Long, contained: Boolean) =
      queryPlanForPrefix(week, (lx, ux), (ly, uy), (lt, ut), z3table,
        kvsToFeatures, iterators, colFamily, numThreads, contained)

    val epochWeekStart = Weeks.weeksBetween(Z3Table.EPOCH, interval.getStart)
    val epochWeekEnd = Weeks.weeksBetween(Z3Table.EPOCH, interval.getEnd)
    val weeks = scala.Range.inclusive(epochWeekStart.getWeeks, epochWeekEnd.getWeeks)
    val lt = Z3Table.secondsInCurrentWeek(interval.getStart, epochWeekStart)
    val ut = Z3Table.secondsInCurrentWeek(interval.getEnd, epochWeekEnd)

    // the z3 index breaks time into 1 week chunks, so create a query plan for each week in our range
    if (weeks.length == 1) {
      Seq(qp(weeks.head, lt, ut, contained = false))
    } else {
      val head +: xs :+ last = weeks.toList
      // time range for a chunk is 0 to 1 week (in seconds)
      val timeMax = Weeks.ONE.toStandardSeconds.getSeconds
      val startQP = qp(head, lt, timeMax, contained = false)
      val endQP = qp(last, 0, ut, contained = false)
      val middleQPs = xs.map(w => qp(w, 0, timeMax, contained = true))
      Seq(startQP, endQP) ++ middleQPs
    }
  }

  def queryPlanForPrefix(week: Int,
                         x: (Double, Double),
                         y: (Double, Double),
                         t: (Long, Long),
                         table: String,
                         kvsToFeatures: FeatureFunction,
                         is: Seq[IteratorSetting],
                         colFamily: Text,
                         numThreads: Int,
                         contained: Boolean = true) = {
    val epochWeekStart = Weeks.weeks(week)
    val prefix = Shorts.toByteArray(epochWeekStart.getWeeks.toShort)

    val accRanges = Z3_CURVE.ranges(x, y, t).map { case (s, e) =>
      val startRowBytes = Bytes.concat(prefix, Longs.toByteArray(s))
      val endRowBytes = Bytes.concat(prefix, Longs.toByteArray(e))
      val start = new Text(startRowBytes)
      val end = Range.followingPrefix(new Text(endRowBytes))
      new Range(start, true, end, false)
    }

    val ll = Z3_CURVE.index(x._1, y._1, t._1)
    val ur = Z3_CURVE.index(x._2, y._2, t._2)
    val iter = Z3Iterator.configure(ll, ur, Z3_ITER_PRIORITY)

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
  override def getCost(filter: QueryFilter, sft: SimpleFeatureType, hints: StrategyHints) = 200
}
