/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka09

import java.awt.RenderingHints.Key
import java.io.{Closeable, Serializable}
import java.{util => ju}

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.google.common.collect.Lists
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.data.{DataStore, DataStoreFactorySpi, Parameter, Query}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.kafka.KafkaDataStoreHelper
import org.locationtech.geomesa.kafka09.KafkaDataStore.FeatureSourceFactory
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

  private val featureSourceCache: LoadingCache[FeatureSourceCacheKey, ContentFeatureSource with Closeable] =
    Caffeine.newBuilder().build[FeatureSourceCacheKey, ContentFeatureSource with Closeable](
      new CacheLoader[FeatureSourceCacheKey, ContentFeatureSource with Closeable] {
        override def load(key: FeatureSourceCacheKey) = {
          fsFactory(key.entry, key.query, kds)
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
    if (query.getStartIndex == null && !query.equals(Query.ALL)) query.setStartIndex(0)
    featureSourceCache.get(FeatureSourceCacheKey(entry, query))
  }

  override def dispose(): Unit = {
    import scala.collection.JavaConversions._

    super[ContentDataStore].dispose()
    super[KafkaDataStoreSchemaManager].dispose()
    featureSourceCache.asMap().values.foreach(_.close())
    featureSourceCache.asMap().clear()
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
  val CLEANUP_LIVE_CACHE   = new Param("cleanUpCache", classOf[java.lang.Boolean], "Run a thread to clean up the live feature cache if set to true. False by default. use 'cleanUpCachePeriod' to configure the length of time between cache cleanups. Every second by default", false)
  val CACHE_CLEANUP_PERIOD = new Param("cleanUpCachePeriod", classOf[String], "Configure the time period between cache cleanups. Default is every ten seconds. This parameter is not used if 'cleanUpCache' is false.", false, "10s")
  val USE_CQ_LIVE_CACHE  = new Param("useCQCache", classOf[java.lang.Boolean], "Use CQEngine-based implementation of live feature cache. False by default.", false, false)
  val COLLECT_QUERY_STAT = new Param("collectQueryStats", classOf[java.lang.Boolean], "Enable monitoring stats for feature store.", false)
  val AUTO_OFFSET_RESET  = new Param("autoOffsetReset", classOf[java.lang.String], "What offset to reset to when there is no initial offset in ZooKeeper or if an offset is out of range. Default is largest.", false, "largest", Map(Parameter.OPTIONS -> Lists.newArrayList("largest","smallest")).asJava)

}

object KafkaDataStore {
  type FeatureSourceFactory = (ContentEntry, Query, KafkaDataStoreSchemaManager) => ContentFeatureSource with Closeable
}

/** A [[DataStoreFactorySpi]] to create a [[KafkaDataStore]] in either producer or consumer mode */
class KafkaDataStoreFactory extends DataStoreFactorySpi {

  import KafkaDataStore._
  import KafkaDataStoreFactoryParams._

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

  override def getDisplayName: String = "Kafka (GeoMesa)"
  override def getDescription: String = "Apache Kafka\u2122 distributed messaging queue"

  override def getParametersInfo: Array[Param] =
    Array(KAFKA_BROKER_PARAM, ZOOKEEPERS_PARAM, ZK_PATH, EXPIRATION_PERIOD, CLEANUP_LIVE_CACHE, CACHE_CLEANUP_PERIOD, USE_CQ_LIVE_CACHE, TOPIC_PARTITIONS, TOPIC_REPLICATION, NAMESPACE_PARAM, COLLECT_QUERY_STAT, AUTO_OFFSET_RESET)

  override def canProcess(params: ju.Map[String, Serializable]): Boolean =
    params.containsKey(KAFKA_BROKER_PARAM.key) && params.containsKey(ZOOKEEPERS_PARAM.key)

  override def isAvailable: Boolean = true
  override def getImplementationHints: ju.Map[Key, _] = null
}
