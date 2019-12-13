/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.data

import java.util.concurrent.atomic.AtomicLong

import org.geotools.data.store.{ContentEntry, ContentFeatureSource, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.arrow.ArrowProperties
import org.locationtech.geomesa.arrow.io.{SimpleFeatureArrowFileReader, SimpleFeatureArrowFileWriter}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class ArrowFeatureSource(entry: ContentEntry, reader: SimpleFeatureArrowFileReader)
    extends ContentFeatureSource(entry, Query.ALL) {

  override def buildFeatureType(): SimpleFeatureType = reader.sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = null

  override def getCountInternal(query: Query): Int = -1

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val features = reader.features(query.getFilter)
    new FeatureReader[SimpleFeatureType, SimpleFeature] {
      override def getFeatureType: SimpleFeatureType = reader.sft
      override def hasNext: Boolean = features.hasNext
      override def next(): SimpleFeature = features.next()
      override def close(): Unit = features.close()
    }
  }

  override def canFilter: Boolean = true

  override def canReproject: Boolean = false
  override def canLimit: Boolean = false
  override def canSort: Boolean = false
  override def canRetype: Boolean = false
  override def canLock: Boolean = false
  override def canTransact: Boolean = false
}

class ArrowFeatureStore(entry: ContentEntry, reader: SimpleFeatureArrowFileReader)
    extends ContentFeatureStore(entry, Query.ALL) {

  import org.locationtech.geomesa.arrow.allocator

  private val delegate = new ArrowFeatureSource(entry, reader)

  private val featureIds = new AtomicLong(0)

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    require(flags != 0, "no write flags set")
    require((flags | WRITER_ADD) == WRITER_ADD, "Only append supported")

    val sft = delegate.getSchema
    val os = entry.getDataStore.asInstanceOf[ArrowDataStore].createOutputStream(true)

    val writer = SimpleFeatureArrowFileWriter(sft, os, encoding = SimpleFeatureEncoding.Max)
    val flushCount = ArrowProperties.BatchSize.get.toLong

    new FeatureWriter[SimpleFeatureType, SimpleFeature] {
      private var count = 0L
      private var feature: ScalaSimpleFeature = _

      override def getFeatureType: SimpleFeatureType = writer.sft

      override def hasNext: Boolean = false

      override def next(): SimpleFeature = {
        feature = new ScalaSimpleFeature(writer.sft, featureIds.getAndIncrement().toString)
        feature
      }

      override def write(): Unit = {
        writer.add(feature)
        feature = null
        count += 1
        if (count % flushCount == 0) {
          writer.flush()
        }
      }

      override def remove(): Unit = throw new NotImplementedError()

      override def close(): Unit = writer.close()
    }
  }

  override def buildFeatureType(): SimpleFeatureType = delegate.buildFeatureType()

  override def getBoundsInternal(query: Query): ReferencedEnvelope = delegate.getBoundsInternal(query)

  override def getCountInternal(query: Query): Int = delegate.getCountInternal(query)

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] =
    delegate.getReaderInternal(query)

  override def canFilter: Boolean = delegate.canFilter
  override def canReproject: Boolean = delegate.canReproject
  override def canLimit: Boolean = delegate.canLimit
  override def canSort: Boolean = delegate.canSort
  override def canRetype: Boolean = delegate.canRetype
  override def canLock: Boolean = delegate.canLock
  override def canTransact: Boolean = delegate.canTransact
}
