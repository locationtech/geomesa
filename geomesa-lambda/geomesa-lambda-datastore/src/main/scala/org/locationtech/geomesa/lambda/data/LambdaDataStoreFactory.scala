/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import java.awt.RenderingHints.Key
import java.io.{Serializable, StringReader}
import java.time.Clock
import java.util.{Collections, Properties}

import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi, Parameter}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStoreFactory, AccumuloDataStoreParams}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.lambda.data.LambdaDataStore.LambdaConfig
import org.locationtech.geomesa.lambda.stream.{OffsetManager, ZookeeperOffsetManager}
import org.locationtech.geomesa.lambda.stream.kafka.KafkaStore

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

class LambdaDataStoreFactory extends DataStoreFactorySpi {

  import LambdaDataStoreFactory.Params._
  import LambdaDataStoreFactory.parsePropertiesParam

  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {
    val brokers = Kafka.BrokersParam.lookUp(params).asInstanceOf[String]
    val expiry = try { Duration(ExpiryParam.lookUp(params).asInstanceOf[String]) } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Couldn't parse expiry parameter: ${ExpiryParam.lookUp(params)}", e)
    }
    val persist = Option(PersistParam.lookUp(params).asInstanceOf[java.lang.Boolean]).forall(_.booleanValue)
    val partitions = Option(Kafka.PartitionsParam.lookUp(params).asInstanceOf[Integer]).map(_.intValue).getOrElse(1)

    val consumerConfig = parsePropertiesParam(Kafka.ConsumerParam.lookUp(params).asInstanceOf[String]) ++
        Map("bootstrap.servers" -> brokers)
    val producer = {
      val producerConfig = parsePropertiesParam(Kafka.ProducerParam.lookUp(params).asInstanceOf[String]) ++
          Map("bootstrap.servers" -> brokers)
      KafkaStore.producer(producerConfig)
    }

    // TODO GEOMESA-1891 attribute level vis
    // TODO GEOMESA-1892 apply default visibilities/auths to kafka store
    val persistence = new AccumuloDataStoreFactory().createDataStore(filter("accumulo", params))

    val zkNamespace = s"gm_lambda_${persistence.config.catalog}"

    val zk = Kafka.ZookeepersParam.lookUp(params).asInstanceOf[String]

    val offsetManager = Option(OffsetManagerParam.lookUp(params).asInstanceOf[OffsetManager])
        .getOrElse(new ZookeeperOffsetManager(zk, zkNamespace))

    val clock = Option(ClockParam.lookUp(params).asInstanceOf[Clock]).getOrElse(Clock.systemUTC())

    val config = LambdaConfig(zk, zkNamespace, partitions, expiry, persist)

    new LambdaDataStore(persistence, producer, consumerConfig, offsetManager, config)(clock)
  }

  override def createNewDataStore(params: java.util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def canProcess(params: java.util.Map[String, Serializable]): Boolean =
    AccumuloDataStoreFactory.canProcess(filter("accumulo", params)) &&
        Seq(ExpiryParam, Kafka.BrokersParam, Kafka.ZookeepersParam).forall(p => params.containsKey(p.getName))

  override def getParametersInfo: Array[Param] = Array(
    LambdaDataStoreFactory.Params.Accumulo.InstanceParam,
    LambdaDataStoreFactory.Params.Accumulo.ZookeepersParam,
    LambdaDataStoreFactory.Params.Accumulo.CatalogParam,
    LambdaDataStoreFactory.Params.Accumulo.UserParam,
    LambdaDataStoreFactory.Params.Accumulo.PasswordParam,
    LambdaDataStoreFactory.Params.Accumulo.KeytabParam,
    LambdaDataStoreFactory.Params.Kafka.BrokersParam,
    LambdaDataStoreFactory.Params.Kafka.ZookeepersParam,
    LambdaDataStoreFactory.Params.ExpiryParam,
    LambdaDataStoreFactory.Params.PersistParam,
    LambdaDataStoreFactory.Params.AuthsParam,
    LambdaDataStoreFactory.Params.EmptyAuthsParam,
    LambdaDataStoreFactory.Params.Accumulo.QueryTimeoutParam,
    LambdaDataStoreFactory.Params.Accumulo.QueryThreadsParam,
    LambdaDataStoreFactory.Params.Accumulo.RecordThreadsParam,
    LambdaDataStoreFactory.Params.Accumulo.WriteThreadsParam,
    LambdaDataStoreFactory.Params.Kafka.PartitionsParam,
    LambdaDataStoreFactory.Params.Kafka.ProducerParam,
    LambdaDataStoreFactory.Params.Kafka.ConsumerParam,
    LambdaDataStoreFactory.Params.VisibilitiesParam,
    LambdaDataStoreFactory.Params.LooseBBoxParam,
    LambdaDataStoreFactory.Params.GenerateStatsParam,
    LambdaDataStoreFactory.Params.AuditQueriesParam
  )

  override def getDisplayName: String = LambdaDataStoreFactory.DisplayName

  override def getDescription: String = LambdaDataStoreFactory.Description

  override def isAvailable: Boolean = true

  override def getImplementationHints: java.util.Map[Key, _] =
    java.util.Collections.EMPTY_MAP.asInstanceOf[java.util.Map[Key, _]]
}

object LambdaDataStoreFactory {

  object Params {

    object Accumulo {
      val InstanceParam      = copy("accumulo", AccumuloDataStoreParams.instanceIdParam)
      val ZookeepersParam    = copy("accumulo", AccumuloDataStoreParams.zookeepersParam)
      val CatalogParam       = copy("accumulo", AccumuloDataStoreParams.tableNameParam)
      val UserParam          = copy("accumulo", AccumuloDataStoreParams.userParam)
      val PasswordParam      = copy("accumulo", AccumuloDataStoreParams.passwordParam)
      val KeytabParam        = copy("accumulo", AccumuloDataStoreParams.keytabPathParam)
      val QueryTimeoutParam  = copy("accumulo", AccumuloDataStoreParams.queryTimeoutParam)
      val QueryThreadsParam  = copy("accumulo", AccumuloDataStoreParams.queryThreadsParam)
      val RecordThreadsParam = copy("accumulo", AccumuloDataStoreParams.recordThreadsParam)
      val WriteThreadsParam  = copy("accumulo", AccumuloDataStoreParams.writeThreadsParam)
      val MockParam          = copy("accumulo", AccumuloDataStoreParams.mockParam)
    }

    object Kafka {
      val BrokersParam    = new Param("kafka.brokers", classOf[String], "Kafka brokers", true)
      val ZookeepersParam = new Param("kafka.zookeepers", classOf[String], "Kafka zookeepers", true)
      val PartitionsParam = new Param("kafka.partitions", classOf[Integer], "Number of partitions to use in kafka queues", false, Int.box(1))
      val ProducerParam   = new Param("kafka.producer.options", classOf[String], "Kafka producer configuration options, in Java properties format", false, null, Collections.singletonMap(Parameter.IS_LARGE_TEXT, java.lang.Boolean.TRUE))
      val ConsumerParam   = new Param("kafka.consumer.options", classOf[String], "Kafka consumer configuration options, in Java properties format'", false, null, Collections.singletonMap(Parameter.IS_LARGE_TEXT, java.lang.Boolean.TRUE))
    }

    val ExpiryParam        = new Param("expiry", classOf[String], "Duration before features expire from transient store. Use 'Inf' to prevent this store from participating in feature expiration", true, "1h")
    val PersistParam       = new Param("persist", classOf[java.lang.Boolean], "Whether to persist expired features to long-term storage", false, java.lang.Boolean.TRUE)
    val VisibilitiesParam  = AccumuloDataStoreParams.visibilityParam
    val AuthsParam         = AccumuloDataStoreParams.authsParam
    val EmptyAuthsParam    = AccumuloDataStoreParams.forceEmptyAuthsParam
    val LooseBBoxParam     = GeoMesaDataStoreFactory.LooseBBoxParam
    val GenerateStatsParam = GeoMesaDataStoreFactory.GenerateStatsParam
    val AuditQueriesParam  = GeoMesaDataStoreFactory.AuditQueriesParam

    // test params
    val ClockParam         = new Param("clock", classOf[Clock], "Clock instance to use for timing", false)
    val OffsetManagerParam = new Param("offsetManager", classOf[OffsetManager], "Offset manager instance to use", false)

    private [data] def copy(ns: String, p: Param): Param =
      new Param(s"$ns.${p.key}", p.`type`, p.title, p.description, p.required, p.minOccurs, p.maxOccurs, p.sample, p.metadata)

    private [data] def filter(ns: String, params: java.util.Map[String, Serializable]): java.util.Map[String, Serializable] = {
      // note: includes a bit of redirection to allow us to pass non-serializable values in to tests
      import scala.collection.JavaConverters._
      Map[String, Any](params.asScala.toSeq: _ *)
          .map { case (k, v) => (if (k.startsWith(s"$ns.")) { k.substring(ns.length + 1) } else { k }, v) }
          .asJava.asInstanceOf[java.util.Map[String, Serializable]]
    }
  }

  private val DisplayName = "Kafka/Accumulo Lambda (GeoMesa)"

  private val Description = "Hybrid store using Kafka for recent events and Accumulo for long-term storage"

  def parsePropertiesParam(value: String): Map[String, String] = {
    import scala.collection.JavaConversions._
    if (value == null || value.isEmpty) { Map.empty } else {
      val props = new Properties()
      props.load(new StringReader(value))
      props.entrySet().map(e => e.getKey.asInstanceOf[String] -> e.getValue.asInstanceOf[String]).toMap
    }
  }
}
