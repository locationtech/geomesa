/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.data

import java.util.concurrent.atomic.AtomicLong

import org.geotools.data.store.{ContentEntry, ContentFeatureSource, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.arrow.io.{SimpleFeatureArrowFileReader, SimpleFeatureArrowFileWriter}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class ArrowFeatureSource(entry: ContentEntry) extends ContentFeatureSource(entry, Query.ALL) {

  import org.locationtech.geomesa.arrow.allocator

  private [data] val ds = entry.getDataStore.asInstanceOf[ArrowDataStore]

  override def buildFeatureType(): SimpleFeatureType = ds.getSchema()

  override def getBoundsInternal(query: Query): ReferencedEnvelope = null

  override def getCountInternal(query: Query): Int = -1

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val reader = new SimpleFeatureArrowFileReader(ds.createInputStream(), query.getFilter)
    val features = reader.features

    new FeatureReader[SimpleFeatureType, SimpleFeature] {
      override def getFeatureType: SimpleFeatureType = reader.sft
      override def hasNext: Boolean = features.hasNext
      override def next(): SimpleFeature = features.next()
      override def close(): Unit = reader.close()
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

class ArrowFeatureStore(entry: ContentEntry) extends ContentFeatureStore(entry, Query.ALL) {

  import org.locationtech.geomesa.arrow.allocator

  private val delegate = new ArrowFeatureSource(entry)

  private val featureIds = new AtomicLong(0)

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    require(flags != 0, "no write flags set")
    require((flags | WRITER_ADD) == WRITER_ADD, "Only append supported")

    val sft = delegate.ds.getSchema
    // TODO suport appending to an existing file
    val os = delegate.ds.createOutputStream(false)
    val writer = new SimpleFeatureArrowFileWriter(sft, os)
    val flushCount = SystemProperty("geomesa.arrow.batch.size", "10000").get.toLong

    new FeatureWriter[SimpleFeatureType, SimpleFeature] {
      private var count = 0L
      private var feature: ScalaSimpleFeature = _

      override def getFeatureType: SimpleFeatureType = writer.sft

      override def hasNext: Boolean = false

      override def next(): SimpleFeature = {
        feature = new ScalaSimpleFeature(featureIds.getAndIncrement().toString, writer.sft)
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
}
