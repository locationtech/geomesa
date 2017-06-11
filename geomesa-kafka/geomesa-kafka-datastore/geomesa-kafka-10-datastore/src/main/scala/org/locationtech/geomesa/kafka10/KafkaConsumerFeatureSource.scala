/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka10

import java.io.Serializable
import java.{util => ju}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Envelope
import org.geotools.data.Query
import org.geotools.data.store.{ContentEntry, ContentFeatureSource}
import org.geotools.feature.NameImpl
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.kafka10.KafkaDataStoreFactoryParams
import org.locationtech.geomesa.kafka10.KafkaDataStore.FeatureSourceFactory
import org.locationtech.geomesa.kafka10.consumer.KafkaConsumerFactory
import org.locationtech.geomesa.security.ContentFeatureSourceSecuritySupport
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.text.Suffixes
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._

import scala.collection.JavaConversions._

abstract class KafkaConsumerFeatureSource(entry: ContentEntry,
                                          sft: SimpleFeatureType,
                                          query: Query,
                                          monitor: Boolean)
  extends ContentFeatureSource(entry, query)
    with ContentFeatureSourceSecuritySupport
    with ContentFeatureSourceReTypingSupport
    with ContentFeatureSourceInfo {

  import org.locationtech.geomesa.utils.geotools._

  override def getBoundsInternal(query: Query) = KafkaConsumerFeatureSource.wholeWorldBounds

  override def buildFeatureType(): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder()
    builder.init(sft)
    builder.setNamespaceURI(getDataStore.getNamespaceURI)
    sft.getUserData.foreach { case (k, v) => builder.userData(k, v) }
    builder.buildFeatureType()
  }

  override def getCountInternal(query: Query): Int = SelfClosingIterator(getReaderInternal(query)).length

  override val canFilter: Boolean = true

  override def getReaderInternal(query: Query): FR = if (monitor) {
    new MonitoringFeatureReader("Kafka", query, addSupport(query, getReaderForFilter(query.getFilter)))
  } else {
    addSupport(query, getReaderForFilter(query.getFilter))
  }

  def getReaderForFilter(f: Filter): FR

  lazy val fqName = new NameImpl(getDataStore.getNamespaceURI, getSchema.getName.getLocalPart)

  override def getName: Name = fqName

}

object KafkaConsumerFeatureSource {
  lazy val wholeWorldBounds = ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), CRS_EPSG_4326)
}

object KafkaConsumerFeatureSourceFactory extends LazyLogging {

  def apply(brokers: String, zk: String, params: ju.Map[String, Serializable]): FeatureSourceFactory = {

    lazy val expirationPeriod: Option[Long] = {
      Option(KafkaDataStoreFactoryParams.EXPIRATION_PERIOD.lookUp(params)).map(_.toString.toLong).filter(_ > 0)
    }

    val cleanUpCache: Boolean = {
      Option(KafkaDataStoreFactoryParams.CLEANUP_LIVE_CACHE.lookUp(params).asInstanceOf[Boolean]).getOrElse(false)
    }

    val cacheCleanUpPeriod: Long = {
      Option(KafkaDataStoreFactoryParams.CACHE_CLEANUP_PERIOD.lookUp(params).asInstanceOf[String]) match {
        case Some(duration) => Suffixes.Time.millis(duration).getOrElse {
          logger.warn("Unable to parse Cache Cleanup Period. Using default of 10000")
          10000
        }
        case None => 10000
      }
    }

    val useCQCache: Boolean = {
      Option(KafkaDataStoreFactoryParams.USE_CQ_LIVE_CACHE.lookUp(params).asInstanceOf[Boolean]).getOrElse(false)
    }

    val monitor: Boolean = {
      Option(KafkaDataStoreFactoryParams.COLLECT_QUERY_STAT.lookUp(params).asInstanceOf[Boolean]).getOrElse(false)
    }

    val autoOffsetReset: String = {
      Option(KafkaDataStoreFactoryParams.AUTO_OFFSET_RESET.lookUp(params).asInstanceOf[String]).getOrElse("largest")
    }

    (entry: ContentEntry, query: Query, schemaManager: KafkaDataStoreSchemaManager) => {
      val kf = new KafkaConsumerFactory(brokers, zk, autoOffsetReset)
      val fc = schemaManager.getFeatureConfig(entry.getTypeName)

      fc.replayConfig match {
        case None =>
          new LiveKafkaConsumerFeatureSource(entry, fc.sft, fc.topic, kf, expirationPeriod, cleanUpCache, useCQCache, query, monitor, cacheCleanUpPeriod)

        case Some(rc) =>
          val replaySFT = fc.sft
          val liveSFT = schemaManager.getLiveFeatureType(replaySFT)
            .getOrElse(throw new IllegalArgumentException(
              "Cannot create Replay FeatureSource because SFT has not been properly prepared."))
          new ReplayKafkaConsumerFeatureSource(entry, replaySFT, liveSFT, fc.topic, kf, rc, query)
      }
    }
  }
}
