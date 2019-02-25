/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.google.common.cache._
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.simple.DelegateSimpleFeatureReader
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query, QueryCapabilities}
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.FileSystemWriter
import org.locationtech.geomesa.utils.io.{CloseWithLogging, FlushWithLogging}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.concurrent.duration.Duration

class FileSystemFeatureStore(val storage: FileSystemStorage,
                             entry: ContentEntry,
                             query: Query,
                             readThreads: Int,
                             writeTimeout: Duration) extends ContentFeatureStore(entry, query) with LazyLogging {

  private val sft = storage.metadata.sft

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    require(flags != 0, "no write flags set")
    require((flags | WRITER_ADD) == WRITER_ADD, "Only append supported")

    new FeatureWriter[SimpleFeatureType, SimpleFeature] {

      private val removalListener = new RemovalListener[String, FileSystemWriter]() {
        override def onRemoval(notification: RemovalNotification[String, FileSystemWriter]): Unit = {
          if (notification.getCause == RemovalCause.EXPIRED) {
            logger.info(s"Flushing writer for partition: ${notification.getKey}")
            val writer = notification.getValue
            FlushWithLogging(writer)
            CloseWithLogging(writer)
          }
        }
      }

      private val writers =
        CacheBuilder.newBuilder()
          .expireAfterAccess(writeTimeout.toMillis, TimeUnit.MILLISECONDS)
          .removalListener[String, FileSystemWriter](removalListener)
          .build(new CacheLoader[String, FileSystemWriter]() {
            override def load(partition: String): FileSystemWriter = storage.getWriter(partition)
          })

      private val featureIds = new AtomicLong(0)
      private var feature: SimpleFeature = _

      override def getFeatureType: SimpleFeatureType = sft

      override def hasNext: Boolean = false

      override def next(): SimpleFeature = {
        feature = new ScalaSimpleFeature(sft, featureIds.getAndIncrement().toString)
        feature
      }

      override def write(): Unit = {
        val partition = storage.metadata.scheme.getPartitionName(feature)
        writers.get(partition).write(feature)
        feature = null
      }

      override def remove(): Unit = throw new NotImplementedError()

      override def close(): Unit = {
        import scala.collection.JavaConversions._
        writers.asMap().values().foreach { writer =>
          FlushWithLogging(writer)
          CloseWithLogging(writer)
        }
      }
    }
  }

  override def buildFeatureType(): SimpleFeatureType = sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = {
    import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326
    val envelope = new ReferencedEnvelope(CRS_EPSG_4326)
    storage.getPartitions(query.getFilter).foreach { partition =>
      partition.bounds.foreach(b => envelope.expandToInclude(b.envelope))
    }
    envelope
  }

  override def getCountInternal(query: Query): Int =
    storage.getPartitions(query.getFilter).map(_.count).sum.toInt

  override def getReaderInternal(original: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    import org.locationtech.geomesa.index.conf.QueryHints._

    val query = new Query(original)
    // The type name can sometimes be empty such as Query.ALL
    query.setTypeName(sft.getTypeName)

    val iter = new FileSystemFeatureIterator(storage, query, readThreads)

    // transforms will be set after getting the iterator
    val transformSft = query.getHints.getTransformSchema.getOrElse(sft)

    // note: DelegateSimpleFeatureIterator will close the iterator by checking that it implements Closeable
    new DelegateSimpleFeatureReader(transformSft, new DelegateSimpleFeatureIterator(iter))
  }

  override def canLimit: Boolean = false
  override def canTransact: Boolean = false
  override def canEvent: Boolean = false
  override def canReproject: Boolean = false
  override def canSort: Boolean = false

  override def canRetype: Boolean = true
  override def canFilter: Boolean = true

  override protected def buildQueryCapabilities(): QueryCapabilities = FileSystemFeatureStore.capabilities
}

object FileSystemFeatureStore {
  private val capabilities = new QueryCapabilities() {
    override def isReliableFIDSupported: Boolean = true
    override def isUseProvidedFIDSupported: Boolean = true
  }
}
