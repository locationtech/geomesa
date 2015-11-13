/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.data

import java.lang

import com.datastax.driver.core._
import com.vividsolutions.jts.geom.Envelope
import org.geotools.data.simple.DelegateSimpleFeatureReader
import org.geotools.data.store._
import org.geotools.data.{FeatureWriter => FW, _}
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.{DateTime, Interval}
import org.locationtech.geomesa.filter.FilterHelper
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class CassandraFeatureStore(entry: ContentEntry) extends ContentFeatureStore(entry, Query.ALL) {

  private lazy val contentState = entry.getState(getTransaction).asInstanceOf[CassandraContentState]

  override def getWriterInternal(query: Query, flags: Int): FW[SimpleFeatureType, SimpleFeature] = {
    if((flags | WRITER_ADD) == WRITER_ADD) new AppendFW(contentState.sft, contentState.session)
    else                                   new UpdateFW(contentState.sft, contentState.session)
  }

  override def buildFeatureType(): SimpleFeatureType = contentState.sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = new ReferencedEnvelope(-180.0, 180.0, -90.0, 90.0, DefaultGeographicCRS.WGS84)

  override def getCountInternal(query: Query): Int = {
    // TODO: might overflow
    if(query.equals(Query.ALL) || FilterHelper.isFilterWholeWorld(query.getFilter)) {
      contentState.session.execute(contentState.ALL_COUNT_QUERY.bind()).iterator().next().getLong(0).toInt
    } else {
      val plans = planQuery(query)
      executeGeoTimeCountQuery(query, plans).toInt
    }
  }

  case class RowAndColumnQueryPlan(row: Int, lz3: Long, uz3: Long, contained: Boolean)

  def executeGeoTimeCountQuery(query: Query, plans: Seq[RowAndColumnQueryPlan]) = {
    // TODO: currently overestimates the count in order to increase performance
    // Fix overestimation by pushing geo etc predicates down into the database
    val features = contentState.builderPool.withResource { builder =>
      val futures = plans.map { case RowAndColumnQueryPlan(r, l, u, contained) =>
        val q = contentState.GEO_TIME_COUNT_QUERY.bind(r: Integer, l: lang.Long, u: lang.Long)
        contentState.session.executeAsync(q)
      }
      futures.flatMap { f => f.get().iterator().toList }.map(_.getLong(0)).sum
    }
    features
  }


  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val iter =
      if(query.equals(Query.ALL) || FilterHelper.isFilterWholeWorld(query.getFilter)) {
        getAllFeatures
      } else {
        val plans    = planQuery(query)
        val features = executeGeoTimeQuery(query, plans)
        features.iterator
      }
    new DelegateSimpleFeatureReader(contentState.sft, new DelegateSimpleFeatureIterator(iter))
  }

  def executeGeoTimeQuery(query: Query, plans: Seq[RowAndColumnQueryPlan]): Seq[SimpleFeature] = {
    val features = contentState.builderPool.withResource { builder =>
      val futures = plans.map { case RowAndColumnQueryPlan(r, l, u, contained) =>
        val q = contentState.GEO_TIME_QUERY.bind(r: Integer, l: lang.Long, u: lang.Long)
        (contained, contentState.session.executeAsync(q))
      }
      futures.flatMap { case (contains, fut) =>
        postProcessResults(query, builder, contains, fut)
      }
    }
    features
  }

  val WHOLE_WORLD = new ReferencedEnvelope(-180.0, 180.0, -90.0, 90.0, DefaultGeographicCRS.WGS84)
  def planQuery(query: Query) = {
    import org.locationtech.geomesa.filter._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

    val origBounds = query.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, DefaultGeographicCRS.WGS84).asInstanceOf[Envelope]
    // TODO: currently we assume that the query has a dtg between predicate and a bbox
    val re = WHOLE_WORLD.intersection(new ReferencedEnvelope(origBounds, DefaultGeographicCRS.WGS84))
    val (lx, ly, ux, uy) = (re.getMinX, re.getMinY, re.getMaxX, re.getMaxY)
    val (dtgFilters, _) = partitionPrimaryTemporals(decomposeAnd(query.getFilter), contentState.sft)
    val interval = FilterHelper.extractInterval(dtgFilters, contentState.sft.getDtgField)
    val startWeek = CassandraPrimaryKey.epochWeeks(interval.getStart)
    val sew = startWeek.getWeeks
    val endWeek = CassandraPrimaryKey.epochWeeks(interval.getEnd)
    val eew = endWeek.getWeeks

    val rows = (sew to eew).map { dt => getRowKeys(lx, ly, ux, uy, interval, sew, eew, dt) }

    val plans =
      rows.flatMap { case ((s, e), rowRanges) =>
        planQueryForContiguousRowRange(s, e, rowRanges)
      }
    plans
  }

  def postProcessResults(query: Query, builder: SimpleFeatureBuilder, contains: Boolean, fut: ResultSetFuture): List[SimpleFeature] = {
    val featureIterator = fut.get().iterator().map { r => convertRowToSF(r, builder) }
    val filt = query.getFilter
    val iter =
      if (!contains) featureIterator.filter(f => filt.evaluate(f)).toList
      else featureIterator.toList
    iter
  }

  def planQueryForContiguousRowRange(s: Int, e: Int, rowRanges: Seq[Int]): Seq[RowAndColumnQueryPlan] = {
    rowRanges.flatMap { r =>
      val CassandraPrimaryKey.Key(_, _, _, _, z) = CassandraPrimaryKey.unapply(r)
      val (minx, miny, maxx, maxy) = CassandraPrimaryKey.SFC2D.bound(z)
      val z3ranges =
        org.locationtech.geomesa.cassandra.data.CassandraPrimaryKey.SFC3D.ranges((minx, maxx), (miny, maxy), (s, e))

      z3ranges.map { ir =>
        val (l, u, contains) = ir.tuple
        RowAndColumnQueryPlan(r, l, u, contains)
      }
    }
  }

  def getRowKeys(lx: Double, ly: Double, ux: Double, uy: Double, interval: Interval, sew: Int, eew: Int, dt: Int): ((Int, Int), Seq[Int]) = {
    val dtshift = dt << 16
    val dtg = new DateTime(0).plusWeeks(dt)

    val seconds =
      if (dt != sew && dt != eew) {
        (0, CassandraPrimaryKey.ONE_WEEK_IN_SECONDS)
      } else {
        val starts =
          if (dt == sew) CassandraPrimaryKey.secondsInCurrentWeek(interval.getStart)
          else 0
        val ends =
          if (dt == eew) CassandraPrimaryKey.secondsInCurrentWeek(interval.getEnd)
          else CassandraPrimaryKey.ONE_WEEK_IN_SECONDS
        (starts, ends)
      }
    val zranges = org.locationtech.geomesa.cassandra.data.CassandraPrimaryKey.SFC2D.toRanges(lx, ly, ux, uy)
    val shiftedRanges = zranges.flatMap { ir =>
      val (l, u, _) = ir.tuple
      (l to u).map { i => (dtshift + i).toInt }
    }
    (seconds, shiftedRanges)
  }

  def getAllFeatures = {
    contentState.builderPool.withResource { builder =>
      contentState.session.execute(contentState.ALL_QUERY.bind()).iterator().map { r => convertRowToSF(r, builder) }
    }
  }

  def convertRowToSF(r: Row, builder: SimpleFeatureBuilder): SimpleFeature = {
    val attrs = contentState.deserializers.map { case (d, idx) => d.deserialize(r.getObject(idx + 1)) }
    val fid = r.getString(0)
    builder.reset()
    builder.buildFeature(fid, attrs.toArray)
  }
}
