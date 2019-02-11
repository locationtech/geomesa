/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import java.time.Clock

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import kafka.admin.AdminUtils
import org.apache.kafka.clients.producer.Producer
import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureReader, SimpleFeatureSource, SimpleFeatureWriter}
import org.geotools.feature.FeatureTypes
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.index.geotools.{GeoMesaFeatureReader, GeoMesaFeatureStore}
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats, NoopStats}
import org.locationtech.geomesa.kafka.AdminUtilsVersions
import org.locationtech.geomesa.lambda.data.LambdaDataStore.LambdaConfig
import org.locationtech.geomesa.lambda.data.LambdaFeatureWriter.{AppendLambdaFeatureWriter, ModifyLambdaFeatureWriter}
import org.locationtech.geomesa.lambda.stream.kafka.KafkaStore
import org.locationtech.geomesa.lambda.stream.{OffsetManager, TransientStore}
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.concurrent.duration.Duration

class LambdaDataStore(val persistence: DataStore,
                      producer: Producer[Array[Byte], Array[Byte]],
                      consumerConfig: Map[String, String],
                      offsetManager: OffsetManager,
                      config: LambdaConfig)
                     (implicit clock: Clock = Clock.systemUTC())
    extends DataStore with HasGeoMesaStats with LazyLogging {

  private val authProvider: Option[AuthorizationsProvider] = persistence match {
    case ds: AccumuloDataStore => Some(ds.config.authProvider)
    case _ => None
  }

  private [lambda] val transients = Caffeine.newBuilder().build(new CacheLoader[String, TransientStore] {
    override def load(key: String): TransientStore = {
      val sft = persistence.getSchema(key)
      new KafkaStore(persistence, sft, authProvider, offsetManager, producer, consumerConfig, config)
    }
  })

  override val stats: GeoMesaStats = persistence match {
    case p: HasGeoMesaStats => new LambdaStats(p.stats, transients)
    case _ => NoopStats
  }

  private val runner = new LambdaQueryRunner(persistence, transients, stats)

  def persist(typeName: String): Unit = transients.get(typeName).persist()

  override def getTypeNames: Array[String] = persistence.getTypeNames

  override def getNames: java.util.List[Name] = persistence.getNames

  override def createSchema(sft: SimpleFeatureType): Unit = {
    persistence.createSchema(sft)
    // TODO for some reason lambda qs consumers don't rebalance when the topic is created after the consumers...
    // transients.get(sft.getTypeName).createSchema()
    val topic = KafkaStore.topic(config.zkNamespace, sft)
    KafkaStore.withZk(config.zookeepers) { zk =>
      if (AdminUtils.topicExists(zk, topic)) {
        logger.warn(s"Topic [$topic] already exists - it may contain stale data")
      } else {
        val replication = SystemProperty("geomesa.kafka.replication").option.map(_.toInt).getOrElse(1)
        AdminUtilsVersions.createTopic(zk, topic, config.partitions, replication)
      }
    }
  }

  override def getSchema(typeName: Name): SimpleFeatureType = getSchema(typeName.getLocalPart)

  override def getSchema(typeName: String): SimpleFeatureType = {
    val sft = persistence.getSchema(typeName)
    if (sft != null) {
      // load our transient store so it starts caching features, etc
      transients.get(typeName)
    }
    sft
  }

  override def updateSchema(typeName: Name, featureType: SimpleFeatureType): Unit =
    updateSchema(typeName.getLocalPart, featureType)

  override def updateSchema(typeName: String, featureType: SimpleFeatureType): Unit = {
    CloseWithLogging(transients.get(typeName))
    transients.invalidate(typeName)
    persistence.updateSchema(typeName, featureType)
  }

  override def removeSchema(typeName: Name): Unit = removeSchema(typeName.getLocalPart)

  override def removeSchema(typeName: String): Unit = {
    // note: call transient first, as it may rely on the schema being present
    val transient = transients.get(typeName)
    transient.removeSchema()
    CloseWithLogging(transient)
    transients.invalidate(typeName)
    persistence.removeSchema(typeName)
  }

  override def getFeatureSource(typeName: Name): SimpleFeatureSource = getFeatureSource(typeName.getLocalPart)

  override def getFeatureSource(typeName: String): SimpleFeatureSource =
    new GeoMesaFeatureStore(this, getSchema(typeName), runner)

  override def getFeatureReader(query: Query, transaction: Transaction): SimpleFeatureReader =
    GeoMesaFeatureReader(getSchema(query.getTypeName), query, runner, None, None)

  override def getFeatureWriterAppend(typeName: String, transaction: Transaction): SimpleFeatureWriter =
    new AppendLambdaFeatureWriter(transients.get(typeName))

  override def getFeatureWriter(typeName: String, transaction: Transaction): SimpleFeatureWriter =
    getFeatureWriter(typeName, Filter.INCLUDE, transaction)

  override def getFeatureWriter(typeName: String,
                                filter: Filter,
                                transaction: Transaction): SimpleFeatureWriter= {
    val query = new Query(typeName, filter)
    val features = SelfClosingIterator(getFeatureReader(query, transaction))
    new ModifyLambdaFeatureWriter(transients.get(typeName), features)
  }

  override def dispose(): Unit = {
    import scala.collection.JavaConversions._
    transients.asMap().values().foreach(CloseWithLogging.apply)
    transients.invalidateAll()
    CloseWithLogging(offsetManager)
    CloseWithLogging(producer)
    persistence.dispose()
  }

  override def getInfo: ServiceInfo = {
    val info = new DefaultServiceInfo()
    info.setDescription(s"Features from ${getClass.getSimpleName}")
    info.setSchema(FeatureTypes.DEFAULT_NAMESPACE)
    info
  }

  override def getLockingManager: LockingManager = null
}

object LambdaDataStore {
  case class LambdaConfig(zookeepers: String,
                          zkNamespace: String,
                          partitions: Int,
                          consumers: Int,
                          expiry: Duration,
                          visibility: Option[String],
                          persist: Boolean)
}