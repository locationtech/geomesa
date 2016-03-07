/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import com.amazonaws.services.dynamodbv2.document.{Item, ItemCollection, QueryOutcome, Table}
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.Interval
import org.locationtech.geomesa.dynamo.core.DynamoGeoQuery
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.GenTraversable
import scala.collection.JavaConversions._

class DynamoDBFeatureStore(entry: ContentEntry,
                           sft: SimpleFeatureType,
                           table: Table)
  extends ContentFeatureStore(entry, Query.ALL) with DynamoGeoQuery {

  override protected[this] val primaryKey = DynamoDBPrimaryKey

  private lazy val contentState: DynamoDBContentState = entry.getState(getTransaction).asInstanceOf[DynamoDBContentState]

  override def buildFeatureType(): SimpleFeatureType = contentState.sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = WHOLE_WORLD

  // TODO: getItemCount returns a Long, may need to do something safer
  override def getCountOfAllDynamo: Int = table.getDescription.getItemCount.toInt

  override def getCountInternal(query: Query): Int = getCountInternalDynamo(query)

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    getReaderInternalDynamo(query, contentState)
  }

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    if((flags | WRITER_ADD) == WRITER_ADD) new DynamoDBAppendingFeatureWriter(contentState.sft, contentState.table)
    else                                   new DynamoDBUpdatingFeatureWriter(contentState.sft, contentState.table)
  }

  override def planQuery(query: Query): GenTraversable[HashAndRangeQueryPlan] = {
    import org.locationtech.geomesa.filter._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

    val (lx, ly, ux, uy) = planQuerySpatialBounds(query)
    val (dtgFilters, _) = partitionPrimaryTemporals(decomposeAnd(query.getFilter), contentState.sft)
    val interval = FilterHelper.extractInterval(dtgFilters, contentState.sft.getDtgField)
    val startWeeks: Int = DynamoDBPrimaryKey.epochWeeks(interval.getStart).getWeeks
    val endWeeks:   Int = DynamoDBPrimaryKey.epochWeeks(interval.getEnd).getWeeks

    val zRanges = DynamoDBPrimaryKey.SFC2D.toRanges(lx, ly, ux, uy).toList

    val rows = (startWeeks to endWeeks).map { dt => getRowKeys(zRanges, interval, startWeeks, endWeeks, dt)}

    val plans =
      rows.flatMap { case ((s, e), rowRanges) =>
        planQueryForContiguousRowRange(s, e, rowRanges)
      }
    plans
  }

  def getRowKeys(zRanges: Seq[IndexRange], interval: Interval, sew: Int, eew: Int, dt: Int): ((Int, Int), Seq[Int]) = {
    getRowKeysDynamo(zRanges, interval, sew, eew, dt)
  }

  override def planQueryForContiguousRowRange(s: Int, e: Int, rowRanges: Seq[Int]): Seq[HashAndRangeQueryPlan] = {
    rowRanges.map { r =>
      val DynamoDBPrimaryKey.Key(_, _, _, _, z) = DynamoDBPrimaryKey.unapply(r)
      val (minx, miny, maxx, maxy) = DynamoDBPrimaryKey.SFC2D.bound(z)
      val min = DynamoDBPrimaryKey.SFC3D.index(minx, miny, s).z
      val max = DynamoDBPrimaryKey.SFC3D.index(maxx, maxy, e).z
      HashAndRangeQueryPlan(r, min, max, contained = false)
    }

  }

  def executeGeoTimeQuery(query: Query, plans: GenTraversable[HashAndRangeQueryPlan]): GenTraversable[SimpleFeature] = {
    val results = plans.map { case HashAndRangeQueryPlan(r, l, u, c) =>
      val q = contentState.geoTimeQuery(r, l, u)
      val res = contentState.table.query(q)
      (c, res)
    }
    results.flatMap{ case (contains, fut) =>
      postProcessResults(query, contains, fut)
    }
  }

  override def executeGeoTimeCountQuery(query: Query, plans: GenTraversable[HashAndRangeQueryPlan]): Long = {
    if (plans.size > 10) {
      -1L
    } else {
      plans.map{ case HashAndRangeQueryPlan(r, l, u, c) =>
        val q = contentState.geoTimeCountQuery(r, l, u)
        val res = contentState.table.query(q)
        res.getTotalCount}.sum.toLong
      }
  }

  def postProcessResults(query: Query, contains: Boolean, fut: ItemCollection[QueryOutcome]): Iterator[SimpleFeature] = {
    val sfts = fut.view.toIterator.map(convertItemToSF)
    if (!contains) {
      val filter = query.getFilter
      sfts.filter(filter.evaluate(_))
    } else {
      sfts
    }
  }

  private def convertItemToSF(i: Item): SimpleFeature = {
    contentState.serializer.deserialize(i.getBinary(DynamoDBDataStore.serId))
  }

  override def getAllFeatures: Iterator[SimpleFeature] = contentState.table.scan(contentState.ALL_QUERY).iterator().map(convertItemToSF)

}

