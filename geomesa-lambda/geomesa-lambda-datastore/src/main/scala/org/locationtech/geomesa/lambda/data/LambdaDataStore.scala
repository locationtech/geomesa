/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.geotools.api.data._
import org.geotools.api.feature.`type`.Name
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.data._
import org.geotools.feature.FeatureTypes
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader.HasGeoMesaFeatureReader
import org.locationtech.geomesa.index.geotools.{GeoMesaDataStore, GeoMesaFeatureReader, GeoMesaFeatureStore}
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats, NoopStats}
import org.locationtech.geomesa.index.utils.FeatureWriterHelper
import org.locationtech.geomesa.lambda.data.LambdaDataStore.LambdaConfig
import org.locationtech.geomesa.lambda.data.LambdaFeatureWriter.{AppendLambdaFeatureWriter, ModifyLambdaFeatureWriter, RequiredVisibilityWriter}
import org.locationtech.geomesa.lambda.stream.kafka.KafkaStore
import org.locationtech.geomesa.lambda.stream.{OffsetManager, TransientStore}
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.time.Clock
import java.util.{Collections, Properties}
import scala.concurrent.duration.FiniteDuration

class LambdaDataStore(val persistence: DataStore, offsetManager: OffsetManager, config: LambdaConfig)
    (implicit clock: Clock = Clock.systemUTC()) extends DataStore with HasGeoMesaStats with HasGeoMesaFeatureReader with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  private val authProvider: Option[AuthorizationsProvider] = persistence match {
    // this is a bit of a hack to work around hbase null vs empty auths
    case ds: GeoMesaDataStore[_] if ds.config.authProvider.getAuthorizations != null => Some(ds.config.authProvider)
    case _ => None
  }

  private [lambda] val transients = Caffeine.newBuilder().build[String, TransientStore](
    new CacheLoader[String, TransientStore] {
      override def load(key: String): TransientStore =
        new KafkaStore(persistence, persistence.getSchema(key), authProvider, offsetManager, config)
    }
  )

  override val stats: GeoMesaStats = persistence match {
    case p: HasGeoMesaStats => new LambdaStats(p.stats, transients)
    case _ => NoopStats
  }

  private val runner = new LambdaQueryRunner(this, persistence, transients)

  def persist(typeName: String): Unit = transients.get(typeName).persist()

  override def getTypeNames: Array[String] = persistence.getTypeNames

  override def getNames: java.util.List[Name] = persistence.getNames

  override def createSchema(sft: SimpleFeatureType): Unit = {
    val topic = LambdaDataStore.topic(sft, config.zkNamespace)
    if (topic.contains("/")) {
      // note: kafka doesn't allow slashes in topic names
      throw new IllegalArgumentException(s"Topic cannot contain '/': $topic")
    }
    persistence.createSchema(sft)
    // TODO for some reason lambda qs consumers don't rebalance when the topic is created after the consumers...
    // transients.get(sft.getTypeName).createSchema()
    val props = new Properties()
    config.producerConfig.foreach { case (k, v) => props.put(k, v) }
    WithClose(AdminClient.create(props)) { admin =>
      if (admin.listTopics().names().get.contains(topic)) {
        logger.warn(s"Topic [$topic] already exists - it may contain stale data")
      } else {
        val replication = SystemProperty("geomesa.kafka.replication").option.map(_.toInt).getOrElse(1)
        val newTopic = new NewTopic(topic, config.partitions, replication.toShort)
        admin.createTopics(Collections.singletonList(newTopic)).all().get
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
    val transient = transients.get(typeName)
    if (typeName != featureType.getTypeName) {
      // ensure that we've loaded the entire kafka topic
      logger.debug("Update schema: entering quiet period")
      Thread.sleep(SystemProperty("geomesa.lambda.update.quiet.period", "10 seconds").toDuration.get.toMillis)
      WithClose(transient.read().iterator()) { toPersist =>
        if (toPersist.nonEmpty) {
          logger.debug("Update schema: persisting transient features")
          WithClose(persistence.getFeatureWriter(typeName, Transaction.AUTO_COMMIT)) { writer =>
            val helper = FeatureWriterHelper(writer, useProvidedFids = true)
            toPersist.foreach(helper.write)
          }
        }
      }
    }
    CloseWithLogging(transient)
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
    new GeoMesaFeatureStore(this, getSchema(typeName))

  override def getFeatureReader(query: Query, transaction: Transaction): SimpleFeatureReader =
    getFeatureReader(getSchema(query.getTypeName), transaction, query).reader()

  override private[geomesa] def getFeatureReader(
      sft: SimpleFeatureType,
      transaction: Transaction,
      query: Query): GeoMesaFeatureReader = {
    GeoMesaFeatureReader(sft, query, runner, None)
  }

  override def getFeatureWriterAppend(typeName: String, transaction: Transaction): SimpleFeatureWriter = {
    val transient = transients.get(typeName)
    if (transient.sft.isVisibilityRequired) {
      new AppendLambdaFeatureWriter(transient) with RequiredVisibilityWriter
    } else {
      new AppendLambdaFeatureWriter(transient)
    }
  }

  override def getFeatureWriter(typeName: String, transaction: Transaction): SimpleFeatureWriter =
    getFeatureWriter(typeName, Filter.INCLUDE, transaction)

  override def getFeatureWriter(typeName: String,
                                filter: Filter,
                                transaction: Transaction): SimpleFeatureWriter= {
    val query = new Query(typeName, filter)
    val features = SelfClosingIterator(getFeatureReader(query, transaction))
    val transient = transients.get(typeName)
    if (transient.sft.isVisibilityRequired) {
      new ModifyLambdaFeatureWriter(transient, features) with RequiredVisibilityWriter
    } else {
      new ModifyLambdaFeatureWriter(transient, features)
    }
  }

  override def dispose(): Unit = {
    CloseWithLogging(transients.asMap.asScala.values)
    transients.invalidateAll()
    CloseWithLogging(offsetManager)
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

  val TopicKey = "geomesa.lambda.topic"

  /**
   * Gets the kafka topic configured in the sft, or a default topic if nothing is configured.
   *
   * @param sft simple feature type
   * @param namespace namespace to use for default topic
   * @return
   */
  def topic(sft: SimpleFeatureType, namespace: String): String = {
    sft.getUserData.get(TopicKey) match {
      case topic: String => topic
      case _ => s"${namespace}_${sft.getTypeName}".replaceAll("[^a-zA-Z0-9_\\-]", "_")
    }
  }

  case class LambdaConfig(
      zookeepers: String,
      zkNamespace: String,
      producerConfig: Map[String, String],
      consumerConfig: Map[String, String],
      partitions: Int,
      consumers: Int,
      expiry: Option[FiniteDuration],
      persistBatchSize: Option[Int] = None,
      offsetCommitInterval: FiniteDuration,
    )
}
