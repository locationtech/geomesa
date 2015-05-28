package org.locationtech.geomesa.accumulo.index

import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection, Point}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.Range
import org.apache.hadoop.io.Text
import org.geotools.data.Query
import org.joda.time.Weeks
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import org.locationtech.geomesa.accumulo.iterators.Z3Iterator
import org.locationtech.geomesa.accumulo.{filter, index}
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.iterators.{KryoLazyFilterTransformIterator, LazyFilterTransformIterator}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.spatial.BinarySpatialOperator

import scala.collection.JavaConversions._

class Z3IdxStrategy extends Strategy with Logging with IndexFilterHelpers  {

  import FilterHelper._
  import Z3IdxStrategy._
  import filter._
  val Z3_CURVE = new Z3SFC

  /**
   * Plans the query - strategy implementations need to define this
   */
  override def getQueryPlans(query: Query, queryPlanner: QueryPlanner, output: ExplainerOutputType) = {
    val sft = queryPlanner.sft
    val acc = queryPlanner.acc

    val dtgField = getDtgFieldName(sft)

    val (geomFilters, otherFilters) = partitionGeom(query.getFilter, sft)
    val (temporalFilters, ecqlFilters) = partitionTemporal(otherFilters, dtgField)

    output(s"Geometry filters: $geomFilters")
    output(s"Temporal filters: $temporalFilters")
    output(s"Other filters: $ecqlFilters")

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

    val iterators = {
      val transforms = for {
        tdef <- index.getTransformDefinition(query)
        tsft <- index.getTransformSchema(query)
      } yield { (tdef, tsft) }

      output(s"Transforms: $transforms")
      val ecql = ecqlFilters.length match {
        case 0 => None
        case 1 => Some(ecqlFilters.head)
        case _ => Some(ff.and(ecqlFilters))
      }

      (ecql, transforms) match {
        case (None, None) => None
        case _ =>
          Some(LazyFilterTransformIterator.configure[KryoLazyFilterTransformIterator](sft,
            ecql, transforms, FILTERING_ITER_PRIORITY))
      }
    }

    val returnSft = getTransformSchema(query).getOrElse(sft)
    val z3table = acc.getZ3Table(sft)
    val numThreads = acc.getSuggestedZ3Threads(sft)

    // setup Z3 iterator
    val env = geometryToCover.getEnvelopeInternal
    val (lx, ly, ux, uy) = (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
    val epochWeekStart = Weeks.weeksBetween(Z3Table.EPOCH, interval.getStart)
    val epochWeekEnd = Weeks.weeksBetween(Z3Table.EPOCH, interval.getEnd)
    val weeks = scala.Range.inclusive(epochWeekStart.getWeeks, epochWeekEnd.getWeeks)
    val lt = Z3Table.secondsInCurrentWeek(interval.getStart, epochWeekStart)
    val ut = Z3Table.secondsInCurrentWeek(interval.getEnd, epochWeekStart)
    if (weeks.length == 1) {
      Seq(queryPlanForPrefix(weeks.head, lt ,ut, lx, ly, ux, uy, z3table, returnSft, iterators, numThreads, contained = false))
    } else {
      val oneWeekInSeconds = Weeks.ONE.toStandardSeconds.getSeconds
      val head +: xs :+ last = weeks.toList
      val middleQPs = xs.map { w =>
        queryPlanForPrefix(w, 0, oneWeekInSeconds, lx, ly, ux, uy, z3table, returnSft, iterators, numThreads, contained = true)
      }
      val startQP = queryPlanForPrefix(head, lt, oneWeekInSeconds, lx, ly, ux, uy, z3table, returnSft, iterators, numThreads, contained = false)
      val endQP   = queryPlanForPrefix(last, 0, ut, lx, ly, ux, uy, z3table, returnSft, iterators, numThreads, contained = false)
      Seq(startQP, endQP) ++ middleQPs
    }
  }

  def queryPlanForPrefix(week: Int, lt: Long, ut: Long,
                         lx: Double, ly: Double, ux: Double, uy: Double,
                         table: String, returnSft: SimpleFeatureType, is: Option[IteratorSetting],
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

    val adaptIter = Z3Table.adaptZ3KryoIterator(returnSft)
    val iters = Seq(Some(iter), is).flatten
    BatchScanPlan(table, accRanges, iters, Seq(Z3Table.FULL_CF), adaptIter, numThreads, hasDuplicates = false)
  }
}

object Z3IdxStrategy extends StrategyProvider {

  import filter._

  val Z3_ITER_PRIORITY = 21
  val FILTERING_ITER_PRIORITY = 25

  /**
   * Returns details on a potential strategy if the filter is valid for this strategy.
   *
   * @param filter
   * @param sft
   * @return
   */
  override def getStrategy(filter: Filter, sft: SimpleFeatureType, hints: StrategyHints): Option[StrategyDecision] = {
    val (geomFilter, other) = partitionGeom(filter, sft)
    val dtgFieldName = getDtgFieldName(sft)
    val (temporal, _) = partitionTemporal(other, dtgFieldName)
    if (Z3Table.supports(sft) && geomFilter.nonEmpty && temporal.nonEmpty && spatialFilters(geomFilter.head)) {
      val geom = sft.getGeometryDescriptor.getLocalName
      val e1 = geomFilter.head.asInstanceOf[BinarySpatialOperator].getExpression1
      val e2 = geomFilter.head.asInstanceOf[BinarySpatialOperator].getExpression2
      checkOrder(e1, e2).filter(_.name == geom).map(_ => StrategyDecision(new Z3IdxStrategy, -1))
     } else {
      None
    }
  }
}
