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
import org.locationtech.geomesa.dynamo.core.DynamoGeoQuery
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class CassandraFeatureStore(ent: ContentEntry)
  extends ContentFeatureStore(ent, Query.ALL) with DynamoGeoQuery {

  private lazy val contentState = entry.getState(getTransaction).asInstanceOf[CassandraContentState]

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    getReaderInternal(query, contentState.sft)
  }

  override def getWriterInternal(query: Query, flags: Int): FW[SimpleFeatureType, SimpleFeature] = {
    if((flags | WRITER_ADD) == WRITER_ADD) new AppendFW(contentState.sft, contentState.session)
    else                                   new UpdateFW(contentState.sft, contentState.session)
  }

  override def buildFeatureType(): SimpleFeatureType = contentState.sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = WHOLE_WORLD

  override def getCountOfAll: Int = checkLongToInt(contentState.getCountOfAll)

  override def getCountInternal(query: Query): Int = getCountInternal(query, contentState.sft)

  override def executeGeoTimeCountQuery(query: Query, plans: Iterator[HashAndRangeQueryPlan]): Long = {
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

  override def executeGeoTimeQuery(q: Query,
                                   plans: Iterator[HashAndRangeQueryPlan]): Iterator[SimpleFeature] = {
    val features = contentState.builderPool.withResource { builder =>
      val futures = plans.map { case HashAndRangeQueryPlan(r, l, u, contained) =>
        val cq = contentState.GEO_TIME_QUERY.bind(r: Integer, l: lang.Long, u: lang.Long)
        (contained, contentState.session.executeAsync(cq))
      }
      futures.flatMap { case (contains, fut) =>
        postProcess(q, builder, contains, fut)
      }
    }
    features
  }

  override def getFeaturesInternal: Iterator[SimpleFeature] = {
    contentState.builderPool.withResource { builder =>
      contentState.session.execute(contentState.ALL_QUERY.bind()).iterator().map {
        r => convertRowToSF(r, builder)
      }
    }
  }

  def planQuery(q: Query): Iterator[HashAndRangeQueryPlan] = {
    planQuery(q, contentState.sft)
  }

  def postProcess(q: Query,
                  builder: SimpleFeatureBuilder,
                  contains: Boolean,
                  fut: ResultSetFuture): Iterator[SimpleFeature] = {
    applyFilter(q, contains, fut.get().iterator().map { r => convertRowToSF(r, builder) })
  }

  def convertRowToSF(r: Row, builder: SimpleFeatureBuilder): SimpleFeature = {
    val attrs = contentState.deserializers.map { case (d, idx) => d.deserialize(r.getObject(idx + 1)) }
    val fid = r.getString(0)
    builder.reset()
    builder.buildFeature(fid, attrs.toArray)
  }
}
