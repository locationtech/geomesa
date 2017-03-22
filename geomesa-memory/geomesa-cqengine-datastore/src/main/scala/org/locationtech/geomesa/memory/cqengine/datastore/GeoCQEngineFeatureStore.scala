/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.datastore

import java.util.concurrent.atomic.AtomicLong

import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class GeoCQEngineFeatureStore(engine: GeoCQEngine, entry: ContentEntry, query: Query) extends
  ContentFeatureStore(entry, query) {

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
         new FeatureWriter[SimpleFeatureType, SimpleFeature] {
           private val tempFeatureIds = new AtomicLong(0)

           var currentFeature: SimpleFeature = null

           override def remove(): Unit = ???

           override def next(): SimpleFeature = {
             currentFeature = new ScalaSimpleFeature(tempFeatureIds.getAndIncrement().toString, engine.sft)
             currentFeature
           }

           override def hasNext: Boolean = false

           override def write(): Unit = {
             if (currentFeature != null) {
               engine.update(currentFeature)
             }
           }

           override def getFeatureType: SimpleFeatureType = engine.sft

           override def close(): Unit = { }
         }
  }

  override def buildFeatureType(): SimpleFeatureType = engine.sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = null

  override def getCountInternal(query: Query): Int = engine.getReaderForFilter(query.getFilter).toIterator.size

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] =
    engine.getReaderForFilter(query.getFilter)
}
