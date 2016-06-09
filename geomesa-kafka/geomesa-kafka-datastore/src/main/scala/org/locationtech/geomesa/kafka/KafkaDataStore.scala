/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka

import java.awt.RenderingHints.Key
import java.io.Serializable
import java.{util => ju}

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.data.{DataStore, DataStoreFactorySpi, Query}
import org.geotools.feature.NameImpl
import org.opengis.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.kafka.KafkaDataStore.FeatureSourceFactory
import org.opengis.feature.`type`.Name
import org.opengis.filter.Filter

import scala.collection.JavaConverters._

class KafkaDataStore(override val zookeepers: String,
                     override val zkPath: String,
                     override val partitions: Int,
                     override val replication: Int,
                     fsFactory: FeatureSourceFactory,
                     val namespaceStr: String = null)
  extends ContentDataStore
  with KafkaDataStoreSchemaManager
  with LazyLogging {

  kds =>

  setNamespaceURI(namespaceStr)

  override def createTypeNames() = getNames.asScala.map(name => new NameImpl(getNamespaceURI, name.getLocalPart) : Name).asJava

  case class FeatureSourceCacheKey(entry: ContentEntry, query: Query)
  private val featureSourceCache =
    CacheBuilder.newBuilder().build[FeatureSourceCacheKey, ContentFeatureSource](
      new CacheLoader[FeatureSourceCacheKey, ContentFeatureSource] {
        override def load(key: FeatureSourceCacheKey) = {
          fsFactory(key.entry, kds)
        }
      })


  def getFeatureSource(typeName: String, filt: Filter): ContentFeatureSource = {
    val entry = ensureEntry(new NameImpl(typeName))
    val query = new Query(typeName, filt)
    query.setStartIndex(0)
    createFeatureSource(entry, query)
  }

  override def createFeatureSource(entry: ContentEntry) = createFeatureSource(entry, Query.ALL)
  private def createFeatureSource(entry: ContentEntry, query: Query) = {
    // Query has an issue when start index is not set - hashCode throws an exception
    // However, Query.ALL has an overridden hashCode that ignores startIndex
    if(query.getStartIndex == null && !query.equals(Query.ALL)) query.setStartIndex(0)
    featureSourceCache.get(FeatureSourceCacheKey(entry, query))
  }

  override def dispose(): Unit = {
    super[ContentDataStore].dispose()
    super[KafkaDataStoreSchemaManager].dispose()
  }

  def getKafkaSchema(typeName: String): SimpleFeatureType = getSchema(new NameImpl(typeName))

  override def getSchema(name: Name): SimpleFeatureType = {
    val typeName = name.getLocalPart
    getFeatureConfig(typeName).sft
  }

}

object KafkaDataStoreFactoryParams {
  val KAFKA_BROKER_PARAM = new Param("brokers", classOf[String], "Kafka broker", true)
  val ZOOKEEPERS_PARAM   = new Param("zookeepers", classOf[String], "Zookeepers", true)
  val ZK_PATH            = new Param("zkPath", classOf[String], "Zookeeper discoverable path", false,  KafkaDataStoreHelper.DefaultZkPath)
  val NAMESPACE_PARAM    = new Param("namespace", classOf[String], "Namespace", false)
  val TOPIC_PARTITIONS   = new Param("partitions", classOf[Integer], "Number of partitions to use in kafka topics", false)
  val TOPIC_REPLICATION  = new Param("replication", classOf[Integer], "Replication factor to use in kafka topics", false)
  val IS_PRODUCER_PARAM  = new Param("isProducer", classOf[java.lang.Boolean], "Is Producer", false, false)
  val EXPIRATION_PERIOD  = new Param("expirationPeriod", classOf[java.lang.Long], "Features will be auto-dropped (expired) after this delay in milliseconds. Leave blank or use -1 to not drop features.", false)
  val CLEANUP_LIVE_CACHE = new Param("cleanUpCache", classOf[java.lang.Boolean], "Run a thread to clean up the live feature cache every second if set to true. False by default.", false)
}

object KafkaDataStore {
  type FeatureSourceFactory = (ContentEntry, KafkaDataStoreSchemaManager) => ContentFeatureSource
}

/** A [[DataStoreFactorySpi]] to create a [[KafkaDataStore]] in either producer or consumer mode */
class KafkaDataStoreFactory extends DataStoreFactorySpi {

  import org.locationtech.geomesa.kafka.KafkaDataStore._
  import org.locationtech.geomesa.kafka.KafkaDataStoreFactoryParams._

  override def createDataStore(params: ju.Map[String, Serializable]): DataStore = {
    val broker = KAFKA_BROKER_PARAM.lookUp(params).asInstanceOf[String]
    val zkHost = ZOOKEEPERS_PARAM.lookUp(params).asInstanceOf[String]
    val namespace = Option(NAMESPACE_PARAM.lookUp(params)).map(_.asInstanceOf[String]).orNull
    val zkPath = KafkaDataStoreHelper.cleanZkPath(ZK_PATH.lookUp(params).asInstanceOf[String])

    val partitions  = Option(TOPIC_PARTITIONS.lookUp(params)).map(_.toString.toInt).getOrElse(1)
    val replication = Option(TOPIC_REPLICATION.lookUp(params)).map(_.toString.toInt).getOrElse(1)

    val fsFactory = createFeatureSourceFactory(broker, zkHost, params)

    new KafkaDataStore(zkHost, zkPath, partitions, replication, fsFactory, namespace)
  }

  def createFeatureSourceFactory(brokers: String,
                                 zk: String,
                                 params: ju.Map[String, Serializable]): FeatureSourceFactory = {

    val isProducer = Option(IS_PRODUCER_PARAM.lookUp(params).asInstanceOf[Boolean]).getOrElse(false)

    if (isProducer) {
      KafkaProducerFeatureStoreFactory(brokers)
    } else {
      KafkaConsumerFeatureSourceFactory(brokers, zk, params)
    }
  }

  override def createNewDataStore(params: ju.Map[String, Serializable]): DataStore =
    throw new UnsupportedOperationException

  override def getDisplayName: String = "Kafka Data Store"
  override def getDescription: String = "Kafka Data Store"

  override def getParametersInfo: Array[Param] =
    Array(KAFKA_BROKER_PARAM, ZOOKEEPERS_PARAM, ZK_PATH, EXPIRATION_PERIOD, CLEANUP_LIVE_CACHE, TOPIC_PARTITIONS, TOPIC_REPLICATION, NAMESPACE_PARAM)

  override def canProcess(params: ju.Map[String, Serializable]): Boolean =
    params.containsKey(KAFKA_BROKER_PARAM.key) && params.containsKey(ZOOKEEPERS_PARAM.key)

  override def isAvailable: Boolean = true
  override def getImplementationHints: ju.Map[Key, _] = null
}
