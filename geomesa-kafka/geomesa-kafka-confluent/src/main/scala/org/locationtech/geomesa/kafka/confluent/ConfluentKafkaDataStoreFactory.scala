/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.Schema
import org.geotools.api.data.DataAccessFactory.Param
import org.geotools.api.data.DataStoreFactorySpi
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreInfo
import org.locationtech.geomesa.kafka.data.{KafkaDataStore, KafkaDataStoreFactory, KafkaDataStoreParams}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

import java.awt.RenderingHints
import java.net.URL
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

class ConfluentKafkaDataStoreFactory extends DataStoreFactorySpi {

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: java.util.Map[String, _]): KafkaDataStore =
    createDataStore(params)

  override def createDataStore(params: java.util.Map[String, _]): KafkaDataStore = {
    val config = KafkaDataStoreFactory.buildConfig(params)
    val url = ConfluentKafkaDataStoreFactory.SchemaRegistryUrl.lookup(params)
    val schemaOverridesConfig = ConfluentKafkaDataStoreFactory.SchemaOverrides.lookupOpt(params)
    val schemaOverrides = ConfluentKafkaDataStoreFactory.parseSchemaOverrides(schemaOverridesConfig)

    // keep confluent classes off the classpath for the data store factory so that it can be loaded via SPI
    ConfluentKafkaDataStore(config, url, schemaOverrides)
  }

  override def getDisplayName: String = ConfluentKafkaDataStoreFactory.DisplayName

  override def getDescription: String = ConfluentKafkaDataStoreFactory.Description

  // note: we don't return producer configs, as they would not be used in geoserver
  override def getParametersInfo: Array[Param] =
    Array(ConfluentKafkaDataStoreFactory.ParameterInfo :+ KafkaDataStoreParams.NamespaceParam: _*)

  override def canProcess(params: java.util.Map[String, _]): Boolean =
    ConfluentKafkaDataStoreFactory.canProcess(params)

  override def isAvailable: Boolean = true

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object ConfluentKafkaDataStoreFactory extends GeoMesaDataStoreInfo with LazyLogging {

  override val DisplayName = "Confluent Kafka (GeoMesa)"
  override val Description = "Confluent Apache Kafka\u2122 distributed log"

  val SchemaRegistryUrl =
    new GeoMesaParam[URL](
      "kafka.schema.registry.url",
      "URL to a confluent schema registry server, used to read Confluent schemas (experimental)",
      optional = false
    )

  val SchemaOverrides =
    new GeoMesaParam[String](
      "kafka.schema.overrides",
      "Typesafe configuration defining a map from topic name to schema override (experimental)",
      optional = true,
      largeText = true
    )

  override val ParameterInfo: Array[GeoMesaParam[_ <: AnyRef]] =
    SchemaRegistryUrl +: KafkaDataStoreFactory.ParameterInfo :+ SchemaOverrides

<<<<<<< HEAD
  override def canProcess(params: java.util.Map[String, _]): Boolean =
    KafkaDataStoreParams.Brokers.exists(params) && SchemaRegistryUrl.exists(params)
=======
  override def canProcess(params: java.util.Map[String, _]): Boolean = {
    KafkaDataStoreParams.Brokers.exists(params) &&
      KafkaDataStoreParams.Zookeepers.exists(params) &&
      SchemaRegistryUrl.exists(params)
  }
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

  private[confluent] def parseSchemaOverrides(config: Option[String]): Map[String, (SimpleFeatureType, Schema)] = {
    try {
      config.map {
        ConfigFactory.parseString(_).resolve().getObject("schemas").asScala.toMap.map {
          case (topic, schemaConfig) =>
            try {
              val schema = new Schema.Parser().parse(schemaConfig.render(ConfigRenderOptions.concise()))
              val sft = SchemaParser.schemaToSft(schema, Some(topic))
              topic -> (sft, schema)
            } catch {
              case NonFatal(ex) =>
                throw new IllegalArgumentException(s"Schema override for topic '$topic' is invalid: ${ex.getMessage}")
            }
        }
      }.getOrElse(Map.empty)
    } catch {
      case NonFatal(ex) => throw new IllegalArgumentException(s"Failed to parse schema overrides: ${ex.getMessage}")
    }
  }
}
