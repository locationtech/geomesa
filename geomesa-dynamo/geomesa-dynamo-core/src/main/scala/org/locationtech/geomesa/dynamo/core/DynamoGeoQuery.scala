/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

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
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
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

  def executeGeoTimeQuery(query: Query, plans: GenTraversable[HashAndRangeQueryPlan]): GenTraversable[SimpleFeature]
  def executeGeoTimeCountQuery(query: Query, plans: GenTraversable[HashAndRangeQueryPlan]): Long

  def getCountOfAll: Int

  def getReaderInternal(query: Query, sft: SimpleFeatureType): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val (spatial, other) = partitionPrimarySpatials(query.getFilter, sft)
    val iter: Iterator[SimpleFeature] =
      if(query.equals(Query.ALL) || spatial.exists(FilterHelper.isFilterWholeWorld)) {
        getAllFeatures(other)
      } else {
        val plans = planQuery(query, sft)
        executeGeoTimeQuery(query, plans).toIterator
      }
    new DelegateSimpleFeatureReader(sft, new DelegateSimpleFeatureIterator(iter.asJava))
  }

  def getCountInternal(query: Query, sft: SimpleFeatureType): Int = {
    if (query.equals(Query.ALL) || FilterHelper.isFilterWholeWorld(query.getFilter)) {
      getCountOfAll
    } else {
      val plans = planQuery(query, sft)
      executeGeoTimeCountQuery(query, plans).toInt
    }
  }

  // Query Specific defs
  def planQuery(query: Query, sft: SimpleFeatureType): GenTraversable[HashAndRangeQueryPlan] = {
    val (lx, ly, ux, uy) = planQuerySpatialBounds(query)
    val (dtgFilters, _) = partitionPrimaryTemporals(decomposeAnd(query.getFilter), sft)
    val interval = FilterHelper.extractInterval(dtgFilters, sft.getDtgField)
    val startWeeks: Int = DynamoPrimaryKey.epochWeeks(interval.getStart).getWeeks
    val endWeeks:   Int = DynamoPrimaryKey.epochWeeks(interval.getEnd).getWeeks

    val zRanges = DynamoPrimaryKey.SFC2D.toRanges(lx, ly, ux, uy).toList

    val rows = (startWeeks to endWeeks).map { dt => getRowKeys(zRanges, interval, startWeeks, endWeeks, dt)}

    val plans = rows.flatMap { case ((s, e), rowRanges) =>
      rowRanges.map(row => planQueryForContiguousRowRange(s, e, row))
    }
    plans
  }

  def planQueryForContiguousRowRange(s: Int, e: Int, row: Int): HashAndRangeQueryPlan = {
    val DynamoPrimaryKey.Key(_, _, _, _, z) = DynamoPrimaryKey.unapply(row)
    val (minx, miny, maxx, maxy) = DynamoPrimaryKey.SFC2D.bound(z)
    val min = DynamoPrimaryKey.SFC3D.index(minx, miny, s).z
    val max = DynamoPrimaryKey.SFC3D.index(maxx, maxy, e).z
    HashAndRangeQueryPlan(row, min, max, contained = false)
  }

  def getRowKeys(zRanges: Seq[IndexRange], interval: Interval, sew: Int, eew: Int, dt: Int): ((Int, Int), Seq[Int]) = {
    val dtshift = dt << 16
    val seconds: (Int, Int) =
      if (dt != sew && dt != eew) {
        (0, DynamoPrimaryKey.ONE_WEEK_IN_SECONDS)
      } else {
        val starts = if (dt == sew) DynamoPrimaryKey.secondsInCurrentWeek(interval.getStart) else 0
        val ends   = if (dt == eew) DynamoPrimaryKey.secondsInCurrentWeek(interval.getEnd)   else DynamoPrimaryKey.ONE_WEEK_IN_SECONDS
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

  def applyFilter(query: Query, contains: Boolean, simpleFeatures: Iterator[SimpleFeature]): Iterator[SimpleFeature] = {
    if (!contains) {
      val filter = query.getFilter
      simpleFeatures.filter(filter.evaluate(_))
    } else {
      simpleFeatures
    }
  }

}
