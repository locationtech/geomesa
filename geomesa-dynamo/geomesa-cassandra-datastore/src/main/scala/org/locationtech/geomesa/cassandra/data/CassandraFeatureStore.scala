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
import org.geotools.data.store._
import org.geotools.data.{FeatureWriter => FW, _}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.Interval
import org.locationtech.geomesa.dynamo.core.DynamoGeoQuery
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.GenTraversable
import scala.collection.JavaConversions._

class CassandraFeatureStore(entry: ContentEntry) extends ContentFeatureStore(entry, Query.ALL) with DynamoGeoQuery {

  override protected[this] val primaryKey = CassandraPrimaryKey

  private lazy val contentState = entry.getState(getTransaction).asInstanceOf[CassandraContentState]

  override def getWriterInternal(query: Query, flags: Int): FW[SimpleFeatureType, SimpleFeature] = {
    if((flags | WRITER_ADD) == WRITER_ADD) new AppendFW(contentState.sft, contentState.session)
    else                                   new UpdateFW(contentState.sft, contentState.session)
  }

  override def buildFeatureType(): SimpleFeatureType = contentState.sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = WHOLE_WORLD

  // TODO: might overflow
  override def getCountOfAllDynamo: Int = contentState.session.execute(contentState.ALL_COUNT_QUERY.bind()).iterator().next().getLong(0).toInt

  override def getCountInternal(query: Query): Int = getCountInternalDynamo(query)

  override def executeGeoTimeCountQuery(query: Query, plans: GenTraversable[HashAndRangeQueryPlan]): Long = {
    // TODO: currently overestimates the count in order to increase performance
    // Fix overestimation by pushing geo etc predicates down into the database
    val features = contentState.builderPool.withResource { builder =>
      val futures = plans.map { case HashAndRangeQueryPlan(r, l, u, contained) =>
        val q = contentState.GEO_TIME_COUNT_QUERY.bind(r: Integer, l: lang.Long, u: lang.Long)
        contentState.session.executeAsync(q)
      }
      futures.flatMap { f => f.get().iterator().toList }.map(_.getLong(0)).sum
    }
    features
  }

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    getReaderInternalDynamo(query, contentState)
  }

  override def executeGeoTimeQuery(query: Query, plans: GenTraversable[HashAndRangeQueryPlan]): GenTraversable[SimpleFeature] = {
    val features = contentState.builderPool.withResource { builder =>
      val futures = plans.map { case HashAndRangeQueryPlan(r, l, u, contained) =>
        val q = contentState.GEO_TIME_QUERY.bind(r: Integer, l: lang.Long, u: lang.Long)
        (contained, contentState.session.executeAsync(q))
      }
      futures.flatMap { case (contains, fut) =>
        postProcessResults(query, builder, contains, fut)
      }
    }
    features
  }

  override def planQuery(query: Query): GenTraversable[HashAndRangeQueryPlan] = {
    import org.locationtech.geomesa.filter._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

    // TODO: currently we assume that the query has a dtg between predicate and a bbox
    val (lx, ly, ux, uy) = planQuerySpatialBounds(query)
    val (dtgFilters, _) = partitionPrimaryTemporals(decomposeAnd(query.getFilter), contentState.sft)
    val interval = FilterHelper.extractInterval(dtgFilters, contentState.sft.getDtgField)
    val startWeeks: Int = CassandraPrimaryKey.epochWeeks(interval.getStart).getWeeks
    val endWeeks:   Int = CassandraPrimaryKey.epochWeeks(interval.getEnd).getWeeks

    val zRanges = CassandraPrimaryKey.SFC2D.toRanges(lx, ly, ux, uy).toList

    val rows = (startWeeks to endWeeks).map { dt => getRowKeys(zRanges, interval, startWeeks, endWeeks, dt) }

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

  override def planQueryForContiguousRowRange(s: Int, e: Int, rowRanges: Seq[Int]): Seq[HashAndRangeQueryPlan] = {
    rowRanges.flatMap { r =>
      val CassandraPrimaryKey.Key(_, _, _, _, z) = CassandraPrimaryKey.unapply(r)
      val (minx, miny, maxx, maxy) = CassandraPrimaryKey.SFC2D.bound(z)
      val z3ranges = CassandraPrimaryKey.SFC3D.ranges((minx, maxx), (miny, maxy), (s, e))

      z3ranges.map { ir =>
        val (l, u, contains) = ir.tuple
        HashAndRangeQueryPlan(r, l, u, contains)
      }
    }
  }

  def getRowKeys(zRanges: Seq[IndexRange], interval: Interval, sew: Int, eew: Int, dt: Int): ((Int, Int), Seq[Int]) = {
    getRowKeysDynamo(zRanges, interval, sew, eew, dt)
  }

  def getAllFeatures: Iterator[SimpleFeature] = {
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
