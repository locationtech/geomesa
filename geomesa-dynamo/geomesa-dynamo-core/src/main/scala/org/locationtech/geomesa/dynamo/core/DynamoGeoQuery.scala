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
import org.joda.time.Interval
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConverters.asJavaIteratorConverter

trait DynamoGeoQuery {

  case class HashAndRangeQueryPlan(row: Int, lz3: Long, uz3: Long, contained: Boolean)

  protected val WHOLE_WORLD = new ReferencedEnvelope(-180.0, 180.0, -90.0, 90.0, CRS_EPSG_4326)

  def getFeaturesInternal: Iterator[SimpleFeature]

  def getFeaturesInternal(filter: Seq[Filter]): Iterator[SimpleFeature] = {
    getFeaturesInternal.filter(f => filter.forall(_.evaluate(f)))
  }

  def executeGeoTimeQuery(query: Query, plans: Iterator[HashAndRangeQueryPlan]): Iterator[SimpleFeature]
  def executeGeoTimeCountQuery(query: Query, plans: Iterator[HashAndRangeQueryPlan]): Long

  def getCountOfAll: Int

  def checkLongToInt(l: Long): Int = {
    if (l >= Int.MaxValue) {
      Int.MaxValue
    } else {
      l.toInt
    }
  }

  def getReaderInternal(q: Query, sft: SimpleFeatureType): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val (spatial, other) = partitionPrimarySpatials(q.getFilter, sft)
    val iter: Iterator[SimpleFeature] =
      if(q.equals(Query.ALL) || spatial.exists(FilterHelper.isFilterWholeWorld)) {
        getFeaturesInternal(other)
      } else {
        val plans = planQuery(q, sft)
        executeGeoTimeQuery(q, plans)
      }
    new DelegateSimpleFeatureReader(sft, new DelegateSimpleFeatureIterator(iter.asJava))
  }

  def getCountInternal(query: Query, sft: SimpleFeatureType): Int = {
    if (query.equals(Query.ALL) || FilterHelper.isFilterWholeWorld(query.getFilter)) {
      getCountOfAll
    } else {
      val plans = planQuery(query, sft)
      checkLongToInt(executeGeoTimeCountQuery(query, plans))
    }
  }

  def planQuery(query: Query, sft: SimpleFeatureType): Iterator[HashAndRangeQueryPlan] = {
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
    plans.toIterator
  }

  def planQueryForContiguousRowRange(s: Int, e: Int, row: Int): HashAndRangeQueryPlan = {
    val DynamoPrimaryKey.Key(_, _, _, _, z) = DynamoPrimaryKey.unapply(row)
    val (minx, miny, maxx, maxy) = DynamoPrimaryKey.SFC2D.bound(z)
    val min = DynamoPrimaryKey.SFC3D.index(minx, miny, s).z
    val max = DynamoPrimaryKey.SFC3D.index(maxx, maxy, e).z
    HashAndRangeQueryPlan(row, min, max, contained = false)
  }

  def getRowKeys(zRanges: Seq[IndexRange],
                 interval: Interval,
                 startWeek: Int,
                 endWeek: Int,
                 dt: Int): ((Int, Int), Seq[Int]) = {
    val dtshift = dt << 16
    val seconds: (Int, Int) =
      if (dt != startWeek && dt != endWeek) {
        (0, DynamoPrimaryKey.ONE_WEEK_IN_SECONDS)
      } else {
        val starts = if (dt == startWeek) {
          DynamoPrimaryKey.secondsInCurrentWeek(interval.getStart)
        } else {
          0
        }
        val ends   = if (dt == endWeek)   {
          DynamoPrimaryKey.secondsInCurrentWeek(interval.getEnd)
        } else {
          DynamoPrimaryKey.ONE_WEEK_IN_SECONDS
        }
        (starts, ends)
      }

    val shiftedRanges = zRanges.flatMap { ir =>
      val (l, u, _) = ir.tuple
      (l to u).map { i => (dtshift + i).toInt }
    }

    (seconds, shiftedRanges)
  }

  def planQuerySpatialBounds(query: Query): (Double, Double, Double, Double) = {
    val origBounds = query.getFilter.accept(
      ExtractBoundsFilterVisitor.BOUNDS_VISITOR, CRS_EPSG_4326).asInstanceOf[Envelope]
    val re = WHOLE_WORLD.intersection(new ReferencedEnvelope(origBounds, CRS_EPSG_4326))
    (re.getMinX, re.getMinY, re.getMaxX, re.getMaxY)
  }

  def applyFilter(q: Query, contains: Boolean, features: Iterator[SimpleFeature]): Iterator[SimpleFeature] = {
    if (!contains) {
      val filter = q.getFilter
      features.filter(filter.evaluate(_))
    } else {
      features
    }
  }

}
