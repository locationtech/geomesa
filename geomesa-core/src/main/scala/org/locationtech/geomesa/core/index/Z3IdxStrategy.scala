package org.locationtech.geomesa.core.index

import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection}
import org.apache.accumulo.core.data.Range
import org.apache.hadoop.io.Text
import org.geotools.data.Query
import org.joda.time.{Interval, Weeks}
import org.locationtech.geomesa.core.data.AccumuloConnectorCreator
import org.locationtech.geomesa.core.data.tables.Z3Table
import org.locationtech.geomesa.core.filter
import org.locationtech.geomesa.curve.{Z3Iterator, Z3SFC}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.expression.Literal
import org.opengis.filter.spatial.BinarySpatialOperator
import org.opengis.filter.{Filter, PropertyIsBetween}

class Z3IdxStrategy extends Strategy with Logging with IndexFilterHelpers  {

  import FilterHelper._
  import filter._
  val Z3_CURVE = new Z3SFC

  /**
   * Plans the query - strategy implementations need to define this
   */
  override def getQueryPlan(query: Query, queryPlanner: QueryPlanner, output: ExplainerOutputType) = {
    val sft = queryPlanner.sft
    val acc = queryPlanner.acc

    val dtgField = getDtgFieldName(sft)

    // TODO: Select only the geometry filters which involve the indexed geometry type.
    // https://geomesa.atlassian.net/browse/GEOMESA-200
    // Simiarly, we should only extract temporal filters for the index date field.
    val (geomFilters, otherFilters) = partitionGeom(query.getFilter, sft)
    val (temporalFilters, ecqlFilters) = partitionTemporal(otherFilters, dtgField)

    // TODO: Currently, we assume no additional predicates other than space and time in Z3
    output(s"Geometry filters: $geomFilters")
    output(s"Temporal filters: $temporalFilters")
    output(s"Other filters: $ecqlFilters")

    val tweakedGeomFilters = geomFilters.map(updateTopologicalFilters(_, sft))

    output(s"Tweaked geom filters are $tweakedGeomFilters")

    // standardize the two key query arguments:  polygon and date-range
    val geomsToCover = tweakedGeomFilters.flatMap(decomposeToGeometry)

    output(s"GeomsToCover: $geomsToCover")

    val collectionToCover: Geometry = geomsToCover match {
      case Nil => null
      case seq: Seq[Geometry] => new GeometryCollection(geomsToCover.toArray, geomsToCover.head.getFactory)
    }

    val temporal = extractTemporal(dtgField)(temporalFilters)
    val interval = netInterval(temporal)
    val geometryToCover = netGeom(collectionToCover)

    val filter = buildFilter(geometryToCover, interval)
    // This catches the case when a whole world query slips through DNF/CNF
    // The union on this geometry collection is necessary at the moment but is not true
    // If given spatial predicates like disjoint.
    val ofilter =
      if (isWholeWorld(geometryToCover)) filterListAsAnd(temporalFilters)
      else filterListAsAnd(tweakedGeomFilters ++ temporalFilters)

    if (ofilter.isEmpty) {
      logger.warn(s"Querying Accumulo without SpatioTemporal filter.")
    }

    output(s"Interval:  $interval")
    output(s"Filter: ${Option(filter).getOrElse("No Filter")}")

    // setup Z3 iterator
    val env = geometryToCover.getEnvelopeInternal
    val (lx, ly, ux, uy) = (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
    val epochWeekStart = Weeks.weeksBetween(Z3Table.EPOCH, interval.getStart)
    val epochWeekEnd = Weeks.weeksBetween(Z3Table.EPOCH, interval.getEnd)
    val weeks = scala.Range.inclusive(epochWeekStart.getWeeks, epochWeekEnd.getWeeks)
    if (weeks.length == 1) Seq(queryPlanForPrefix(weeks.head, interval, lx, ly, ux, uy, acc, sft, contained = false))
    else {
      val head +: xs :+ last = weeks.toList
      val middleQPs = xs.map { w => queryPlanForPrefix(w, interval, lx, ly, ux, uy, acc, sft, contained = true) }
      val edgePlans = Seq(head, last).map { w => queryPlanForPrefix(w, interval, lx, ly, ux, uy, acc, sft, contained = false) }
      edgePlans ++ middleQPs
    }
  }

  def queryPlanForPrefix(week: Int, interval: Interval, lx: Double, ly: Double, ux: Double, uy: Double,
          acc: AccumuloConnectorCreator, sft: SimpleFeatureType, 
          contained: Boolean = true) = {
    val epochWeekStart = Weeks.weeks(week)
    val (lt, ut) = 
      if(contained) (0, Weeks.ONE.toStandardSeconds.getSeconds)
      else (
        Z3Table.secondsInCurrentWeek(interval.getStart, epochWeekStart),
        Z3Table.secondsInCurrentWeek(interval.getEnd, epochWeekStart))

    val z3ranges = Z3_CURVE.ranges(lx, ly, ux, uy, lt, ut, 8)

    val prefix = Shorts.toByteArray(epochWeekStart.getWeeks.toShort)

    val accRanges = z3ranges.map { case (s, e) =>
      val startRowBytes = Bytes.concat(prefix, Longs.toByteArray(s))
      val endRowBytes = Bytes.concat(prefix, Longs.toByteArray(e))
      new Range(
        new Text(startRowBytes), true,
        Range.followingPrefix(new Text(endRowBytes)), false)
    }

    val iter = Z3Iterator.configure(Z3_CURVE.index(lx, ly, lt), Z3_CURVE.index(ux, uy, ut), 21)

    val table = acc.getZ3Table(sft)
    BatchScanPlan(table, accRanges, Seq(iter), Seq(Z3Table.BIN_ROW), 8, hasDuplicates = false)
  }
}

object Z3IdxStrategy extends StrategyProvider {
  import FilterHelper._
  import filter._

  /**
   * Returns details on a potential strategy if the filter is valid for this strategy.
   *
   * @param filter
   * @param sft
   * @return
   */
  override def getStrategy(filter: Filter, sft: SimpleFeatureType, hints: StrategyHints): Option[StrategyDecision] = {
    val (geomFilter, other) = partitionGeom(filter, sft)
    val (temporal, _) = partitionTemporal(other, getDtgFieldName(sft))
    if(geomFilter.size == 0 || temporal.size == 0) {
      None
    } else if (spatialFilters(geomFilter.head) && !isFilterWholeWorld(geomFilter.head)) {
      val temporalFilter = temporal.head
      val between = temporalFilter.asInstanceOf[PropertyIsBetween]
      val s = between.getLowerBoundary.asInstanceOf[Literal].getValue
      val e = between.getUpperBoundary.asInstanceOf[Literal].getValue
      val geom = sft.getGeometryDescriptor.getLocalName
      val e1 = geomFilter.head.asInstanceOf[BinarySpatialOperator].getExpression1
      val e2 = geomFilter.head.asInstanceOf[BinarySpatialOperator].getExpression2
      checkOrder(e1, e2).filter(_.name == geom).map(_ => StrategyDecision(new Z3IdxStrategy, -1))
     } else {
      None
    }
  }
}