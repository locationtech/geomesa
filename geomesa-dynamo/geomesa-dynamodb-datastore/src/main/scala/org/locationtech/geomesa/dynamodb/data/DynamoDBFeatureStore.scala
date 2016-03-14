/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import com.amazonaws.services.dynamodbv2.document.{Item, ItemCollection, QueryOutcome}
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.dynamo.core.DynamoGeoQuery
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.GenTraversable
import scala.collection.JavaConversions._

class DynamoDBFeatureStore(entry: ContentEntry) extends ContentFeatureStore(entry, Query.ALL) with DynamoGeoQuery {

  private lazy val contentState: DynamoDBContentState = entry.getState(getTransaction).asInstanceOf[DynamoDBContentState]

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    getReaderInternal(query, contentState.sft)
  }

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    if((flags | WRITER_ADD) == WRITER_ADD) new DynamoDBAppendingFeatureWriter(contentState.sft, contentState.table)
    else                                   new DynamoDBUpdatingFeatureWriter(contentState.sft, contentState.table)
  }

  override def buildFeatureType(): SimpleFeatureType = contentState.sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = WHOLE_WORLD

  // TODO: might overflow
  override def getCountOfAll: Int = contentState.getCountOfAll.toInt

  override def getCountInternal(query: Query): Int = getCountInternal(query, contentState.sft)

  override def executeGeoTimeCountQuery(query: Query, plans: GenTraversable[HashAndRangeQueryPlan]): Long = {
    if (plans.size > 10) {
      -1L
    } else {
      plans.map{ case HashAndRangeQueryPlan(r, l, u, c) =>
        val q = contentState.geoTimeCountQuery(r, l, u)
        val res = contentState.table.query(q)
        res.getTotalCount
      }.sum.toLong
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

  override def getAllFeatures: Iterator[SimpleFeature] = contentState.table.scan(contentState.ALL_QUERY).iterator().map(convertItemToSF)

  def planQuery(query: Query): GenTraversable[HashAndRangeQueryPlan] = {
    planQuery(query, contentState.sft)
  }

  def postProcessResults(query: Query, contains: Boolean, fut: ItemCollection[QueryOutcome]): Iterator[SimpleFeature] = {
    applyFilter(query, contains, fut.view.toIterator.map(convertItemToSF))
  }

  private def convertItemToSF(i: Item): SimpleFeature = {
    contentState.serializer.deserialize(i.getBinary(DynamoDBDataStore.serId))
  }

}

