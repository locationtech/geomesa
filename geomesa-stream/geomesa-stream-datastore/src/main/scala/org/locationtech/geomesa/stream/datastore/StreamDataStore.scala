/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.stream.datastore

import java.awt.RenderingHints
import java.util.concurrent.{CopyOnWriteArrayList, Executors, TimeUnit}
import java.util.logging.Level
import java.{util => ju}

import com.github.benmanes.caffeine.cache.{Cache, Caffeine, RemovalCause, RemovalListener}
import com.google.common.collect.Lists
import com.typesafe.config.ConfigFactory
import com.vividsolutions.jts.geom.Envelope
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureReader
import org.geotools.data.store._
import org.geotools.filter.FidFilterImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.filter.index.SpatialIndexSupport
import org.locationtech.geomesa.stream.SimpleFeatureStreamSource
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.FR
import org.locationtech.geomesa.utils.index.{SpatialIndex, SynchronizedQuadtree}
import org.locationtech.geomesa.utils.geotools.{DFI, DFR, FR}
import org.locationtech.geomesa.utils.index.{SpatialIndex, SynchronizedQuadtree}
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

case class FeatureHolder(sf: SimpleFeature, env: Envelope) {
  override def hashCode(): Int = sf.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: FeatureHolder => sf.equals(other.sf)
    case _ => false
  }
}

class StreamDataStore(source: SimpleFeatureStreamSource, timeout: Int) extends ContentDataStore {
  
  val sft = source.sft
  source.init()
  val qt = new SynchronizedQuadtree[SimpleFeature]

  val cb =
    Caffeine
      .newBuilder()
      .expireAfterWrite(timeout, TimeUnit.SECONDS)
      .removalListener(
        new RemovalListener[String, FeatureHolder] {
          override def onRemoval(k: String, v: FeatureHolder, removalCause: RemovalCause): Unit = {
            qt.remove(v.env, v.sf)
          }
        }
      )

  val features = cb.build[String, FeatureHolder]()

  val listeners = new CopyOnWriteArrayList[StreamListener]()

  private val executor = Executors.newSingleThreadExecutor()
  executor.submit(
    new Runnable {
      override def run(): Unit = {
        while(true) {
          try {
            val sf = source.next
            if(sf != null) {
              val env = sf.geometry.getEnvelopeInternal
              qt.insert(env, sf)
              features.put(sf.getID, FeatureHolder(sf, env))
              listeners.foreach { l =>
                try {
                  l.onNext(sf)
                } catch {
                  case t: Throwable => getLogger.log(Level.WARNING, "Unable to notify listener", t)
                }
              }
            }
          } catch {
            case t: Throwable =>
            // swallow
          }
        }
      }
    }
  )

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource =
    new StreamFeatureStore(entry, null, features, qt, sft)

  def registerListener(listener: StreamListener): Unit = listeners.add(listener)

  override def createTypeNames(): ju.List[Name] = Lists.newArrayList(sft.getName)

  def close(): Unit = {
    try {
      executor.shutdown()
    } catch {
      case t: Throwable => // swallow
    }
  }
}

class StreamFeatureStore(entry: ContentEntry,
                         query: Query,
                         features: Cache[String, FeatureHolder],
                         val spatialIndex: SpatialIndex[SimpleFeature],
                         val sft: SimpleFeatureType)
  extends ContentFeatureStore(entry, query) with SpatialIndexSupport {

  override def canFilter: Boolean = true

  override def getBoundsInternal(query: Query) =
    ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)

  override def buildFeatureType(): SimpleFeatureType = sft

  override def getCountInternal(query: Query): Int =
    getReaderInternal(query).toIterator.size

  override def getReaderInternal(query: Query): FR = getReaderForFilter(query.getFilter)

  override def allFeatures(): Iterator[SimpleFeature] = features.asMap().valuesIterator.map(_.sf)

  override def getReaderForFilter(f: Filter): SimpleFeatureReader =
    f match {
      case id: FidFilterImpl => fid(id)
      case _                 => super.getReaderForFilter(f)
    }

  def fid(ids: FidFilterImpl): SimpleFeatureReader = {
    val iter = ids.getIDs.flatMap(id => Option(features.getIfPresent(id.toString)).map(_.sf)).iterator
    reader(iter)
  }

  override def getWriterInternal(query: Query, flags: Int) = throw new IllegalArgumentException("Not allowed")
}

object StreamDataStoreParams {
  val STREAM_DATASTORE_CONFIG = new Param("geomesa.stream.datastore.config", classOf[String], "", true)
  val CACHE_TIMEOUT = new Param("geomesa.stream.datastore.cache.timeout", classOf[java.lang.Integer], "", true, 10)
}

class StreamDataStoreFactory extends DataStoreFactorySpi {

  import StreamDataStoreParams._

  override def createDataStore(params: ju.Map[String, java.io.Serializable]): DataStore = {
    val confString = STREAM_DATASTORE_CONFIG.lookUp(params).asInstanceOf[String]
    val timeout = Option(CACHE_TIMEOUT.lookUp(params)).map(_.asInstanceOf[Int]).getOrElse(10)
    val conf = ConfigFactory.parseString(confString)
    val source = SimpleFeatureStreamSource.buildSource(conf)
    new StreamDataStore(source, timeout)
  }

  override def createNewDataStore(params: ju.Map[String, java.io.Serializable]): DataStore = ???
  override def getDescription: String = "SimpleFeature Stream Source"
  override def getParametersInfo: Array[Param] = Array(STREAM_DATASTORE_CONFIG, CACHE_TIMEOUT)
  override def getDisplayName: String = "SimpleFeature Stream Source"
  override def canProcess(params: ju.Map[String, java.io.Serializable]): Boolean =
    params.containsKey(STREAM_DATASTORE_CONFIG.key)

  override def isAvailable: Boolean = true
  override def getImplementationHints: ju.Map[RenderingHints.Key, _] = null
}

trait StreamListener {
  def onNext(sf: SimpleFeature): Unit
}

object StreamListener {
  def apply(f: Filter, fn: SimpleFeature => Unit) =
    new StreamListener {
      override def onNext(sf: SimpleFeature): Unit = if(f.evaluate(sf)) fn(sf)
    }

  def apply(fn: SimpleFeature => Unit) =
    new StreamListener {
      override def onNext(sf: SimpleFeature): Unit = fn(sf)
    }
}

