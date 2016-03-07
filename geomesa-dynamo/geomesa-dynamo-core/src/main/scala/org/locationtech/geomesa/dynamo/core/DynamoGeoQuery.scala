package org.locationtech.geomesa.dynamo.core

import com.vividsolutions.jts.geom.Envelope
import org.geotools.data.simple.DelegateSimpleFeatureReader
import org.geotools.data.{FeatureReader, Query}
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.Interval
import org.locationtech.geomesa.filter._
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.{SimpleFeatureType, SimpleFeature}
import org.opengis.filter.Filter

import scala.collection.GenTraversable
import scala.collection.JavaConverters.asJavaIteratorConverter

trait DynamoGeoQuery {

  case class HashAndRangeQueryPlan(row: Int, lz3: Long, uz3: Long, contained: Boolean)

  protected val WHOLE_WORLD = new ReferencedEnvelope(-180.0, 180.0, -90.0, 90.0, DefaultGeographicCRS.WGS84)

  def getAllFeatures: Iterator[SimpleFeature]

  def getAllFeatures(filter: Seq[Filter]): Iterator[SimpleFeature] = {
    getAllFeatures.filter(f => filter.forall(_.evaluate(f)))
  }

  protected val primaryKey: DynamoPrimaryKey

  // Query Specific defs
  def planQuery(query: Query): GenTraversable[HashAndRangeQueryPlan]

  def executeGeoTimeQuery(query: Query, plans: GenTraversable[HashAndRangeQueryPlan]): GenTraversable[SimpleFeature]
  def executeGeoTimeCountQuery(query: Query, plans: GenTraversable[HashAndRangeQueryPlan]): Long

  def getCountOfAllDynamo: Int

  def getReaderInternalDynamo(query: Query, cs: DynamoContentState): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val sft = cs.sft
    val (spatial, other) = partitionPrimarySpatials(query.getFilter, sft)
    val iter: Iterator[SimpleFeature] =
      if(query.equals(Query.ALL) || spatial.exists(FilterHelper.isFilterWholeWorld)) {
        getAllFeatures(other)
      } else {
        val plans = planQuery(query)
        executeGeoTimeQuery(query, plans).toIterator
      }
    new DelegateSimpleFeatureReader(sft, new DelegateSimpleFeatureIterator(iter.asJava))
  }

  def getCountInternalDynamo(query: Query): Int = {
    if (query.equals(Query.ALL) || FilterHelper.isFilterWholeWorld(query.getFilter)) {
      getCountOfAllDynamo
    } else {
      val plans = planQuery(query)
      executeGeoTimeCountQuery(query, plans).toInt
    }
  }


  def planQueryForContiguousRowRange(s: Int, e: Int, rowRanges: Seq[Int]): Seq[HashAndRangeQueryPlan]

  def getRowKeysDynamo(zRanges: Seq[IndexRange], interval: Interval, sew: Int, eew: Int, dt: Int) = {
    val dtshift = dt << 16
    val seconds: (Int, Int) =
      if (dt != sew && dt != eew) {
        (0, primaryKey.ONE_WEEK_IN_SECONDS)
      } else {
        val starts = if (dt == sew) primaryKey.secondsInCurrentWeek(interval.getStart) else 0
        val ends   = if (dt == eew) primaryKey.secondsInCurrentWeek(interval.getEnd)   else primaryKey.ONE_WEEK_IN_SECONDS
        (starts, ends)
      }

    val shiftedRanges = zRanges.flatMap { ir =>
      val (l, u, _) = ir.tuple
      (l to u).map { i => (dtshift + i).toInt }
    }

    (seconds, shiftedRanges)
  }

  def planQuerySpatialBounds(query: Query): (Double, Double, Double, Double) = {
    val origBounds = query.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, DefaultGeographicCRS.WGS84).asInstanceOf[Envelope]
    val re = WHOLE_WORLD.intersection(new ReferencedEnvelope(origBounds, DefaultGeographicCRS.WGS84))
    (re.getMinX, re.getMinY, re.getMaxX, re.getMaxY)
  }


}
