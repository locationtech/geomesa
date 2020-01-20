/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.datastore

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import org.geotools.data.collection.DelegateFeatureReader
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query, QueryCapabilities}
import org.geotools.feature.collection.DelegateFeatureIterator
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.ScalaSimpleFeature.ImmutableSimpleFeature
import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
import org.locationtech.geomesa.memory.cqengine.datastore.GeoCQEngineFeatureStore.{GeoCQEngineFeatureWriter, GeoCQEngineQueryCapabilities}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class GeoCQEngineFeatureStore(engine: GeoCQEngine, entry: ContentEntry, query: Query) extends
  ContentFeatureStore(entry, query) {

  import scala.collection.JavaConverters._

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] =
    new GeoCQEngineFeatureWriter(engine)

  override def buildFeatureType(): SimpleFeatureType = engine.sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = null

  override def getCountInternal(query: Query): Int =
    SelfClosingIterator(engine.query(query.getFilter)).length

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] =
    new DelegateFeatureReader(engine.sft, new DelegateFeatureIterator(engine.query(query.getFilter).asJava))

  override def getQueryCapabilities: QueryCapabilities = GeoCQEngineQueryCapabilities

  override def canEvent: Boolean = true
}

object GeoCQEngineFeatureStore {

  object GeoCQEngineQueryCapabilities extends QueryCapabilities {
    override def isUseProvidedFIDSupported = true
  }

  class GeoCQEngineFeatureWriter(engine: GeoCQEngine) extends FeatureWriter[SimpleFeatureType, SimpleFeature] {

    private val tempFeatureIds = new AtomicLong(0)
    private val currentFeature: ScalaSimpleFeature = new ScalaSimpleFeature(engine.sft, "")
    private var queued = false

    override def getFeatureType: SimpleFeatureType = engine.sft

    override def hasNext: Boolean = false

    override def next(): SimpleFeature = {
      var i = 0
      while (i < engine.sft.getAttributeCount) {
        currentFeature.setAttributeNoConvert(i, null)
        i += 1
      }
      currentFeature.getUserData.clear()
      currentFeature.setId(tempFeatureIds.getAndIncrement().toString)
      queued = true
      currentFeature
    }

    override def write(): Unit = {
      if (!queued) {
        throw new IllegalStateException("Must call 'next' before 'write'")
      }
      val values = Array.tabulate(engine.sft.getAttributeCount)(currentFeature.getAttribute)
      val id = if (currentFeature.getUserData.containsKey(Hints.PROVIDED_FID)) {
        currentFeature.getUserData.get(Hints.PROVIDED_FID).toString
      } else if (currentFeature.getUserData.containsKey(Hints.USE_PROVIDED_FID) &&
          currentFeature.getUserData.get(Hints.USE_PROVIDED_FID).asInstanceOf[Boolean]) {
        currentFeature.getID
      } else {
        UUID.randomUUID.toString
      }
      val userData = new java.util.HashMap[AnyRef, AnyRef](currentFeature.getUserData)
      engine.update(new ImmutableSimpleFeature(engine.sft, id, values, userData))
      queued = false
    }

    override def remove(): Unit = throw new NotImplementedError()

    override def close(): Unit = {}
  }
}
