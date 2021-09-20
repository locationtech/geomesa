/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import com.typesafe.config.{ConfigFactory, ConfigList, ConfigObject, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.StringUtils
import org.geotools.api.data.DataAccessFactory.Param
import org.geotools.api.data.DataStoreFactorySpi
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.SerializationOption
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreInfo
import org.locationtech.geomesa.index.metadata.MetadataStringSerializer
import org.locationtech.geomesa.kafka.data.KafkaDataStore._
import org.locationtech.geomesa.kafka.data.KafkaDataStoreParams.{LazyFeatures, SerializationType}
import org.locationtech.geomesa.kafka.utils.GeoMessageSerializer.GeoMessageSerializerFactory
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType
import org.locationtech.geomesa.metrics.core.GeoMesaMetrics
import org.locationtech.geomesa.security.{AuthUtils, AuthorizationsProvider}
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, NoOpAuditProvider}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.locationtech.geomesa.utils.index.SizeSeparatedBucketIndex
import org.locationtech.geomesa.utils.zk.ZookeeperMetadata
import pureconfig.error.{CannotConvert, ConfigReaderFailures, FailureReason}
<<<<<<< HEAD
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource}
<<<<<<< HEAD

import java.awt.RenderingHints
import java.io.IOException
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5a7ce4912 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd74249075 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
=======
>>>>>>> 642891ac8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5a7ce4912 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd74249075 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
import pureconfig.{ConfigCursor, ConfigReader, Derivation}

import java.awt.RenderingHints
import java.io.{IOException, Serializable}
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5a7ce4912 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd74249075 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 515355cfcd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 515355cfcd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======

import java.awt.RenderingHints
import java.io.IOException
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 642891ac8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5a7ce4912 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> cd74249075 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 515355cfcd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.control.NonFatal

class KafkaDataStoreFactory extends DataStoreFactorySpi {

  import org.locationtech.geomesa.kafka.data.KafkaDataStoreParams._

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: java.util.Map[String, _]): KafkaDataStore =
    createDataStore(params)

  override def createDataStore(params: java.util.Map[String, _]): KafkaDataStore = {
    val config = KafkaDataStoreFactory.buildConfig(params)
    val serializer = KafkaDataStoreFactory.buildSerializer(params)
    val ds = config.zookeepers match {
      case None =>
        val meta = new KafkaMetadata(config, MetadataStringSerializer)
        new KafkaDataStore(config, meta, serializer)

      case Some(zk) =>
        val meta = new ZookeeperMetadata(s"${config.catalog}/$MetadataPath", zk, MetadataStringSerializer)
        val ds = new KafkaDataStoreWithZk(config, meta, serializer, zk)
        // migrate old schemas, if any
        if (!meta.read("migration", "check").exists(_.toBoolean)) {
          new MetadataMigration(ds, config.catalog, zk).run()
          meta.insert("migration", "check", "true")
        }
        ds
    }
    if (!LazyLoad.lookup(params)) {
      ds.startAllConsumers()
    }
    ds
  }

  override def getDisplayName: String = KafkaDataStoreFactory.DisplayName

  override def getDescription: String = KafkaDataStoreFactory.Description

  // note: we don't return producer configs, as they would not be used in geoserver
  override def getParametersInfo: Array[Param] =
    KafkaDataStoreFactory.ParameterInfo :+ NamespaceParam.asInstanceOf[Param]

  override def canProcess(params: java.util.Map[String, _]): Boolean =
    KafkaDataStoreFactory.canProcess(params)

  override def isAvailable: Boolean = true

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object KafkaDataStoreFactory extends GeoMesaDataStoreInfo with LazyLogging {

  import scala.collection.JavaConverters._

<<<<<<< HEAD
  private val LayerViewReader = ConfigReader.fromCursor(readLayerViewConfig)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

<<<<<<< HEAD
  val DefaultCatalog: String = org.locationtech.geomesa.kafka.data.DefaultCatalog
  val DefaultZkPath: String = org.locationtech.geomesa.kafka.data.DefaultZkPath
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cd74249075 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 515355cfcd (GEOMESA-3100 Kafka layer views (#2784))
  val DefaultCatalog: String = "geomesa-catalog"
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b5a7ce4912 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
  private val LayerViewReader = Derivation.Successful(ConfigReader.fromCursor(readLayerViewConfig))
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

<<<<<<< HEAD
<<<<<<< HEAD
  val DefaultCatalog: String = "geomesa-catalog"
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
  private val LayerViewReader = Derivation.Successful(ConfigReader.fromCursor(readLayerViewConfig))
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
  val DefaultZkPath: String = "geomesa/ds/kafka"
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
  val DefaultCatalog: String = org.locationtech.geomesa.kafka.data.DefaultCatalog
  val DefaultZkPath: String = org.locationtech.geomesa.kafka.data.DefaultZkPath
<<<<<<< HEAD
>>>>>>> d40f742b4 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
=======
  val DefaultCatalog: String = "geomesa-catalog"
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
=======
  private val LayerViewReader = Derivation.Successful(ConfigReader.fromCursor(readLayerViewConfig))
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

<<<<<<< HEAD
<<<<<<< HEAD
  val DefaultCatalog: String = "geomesa-catalog"
<<<<<<< HEAD
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
=======
=======
  private val LayerViewReader = Derivation.Successful(ConfigReader.fromCursor(readLayerViewConfig))
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
  val DefaultZkPath: String = "geomesa/ds/kafka"
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
  val DefaultZkPath: String = "geomesa/ds/kafka"
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
=======
  val DefaultCatalog: String = "geomesa-catalog"
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
  private val LayerViewReader = Derivation.Successful(ConfigReader.fromCursor(readLayerViewConfig))
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

<<<<<<< HEAD
<<<<<<< HEAD
  val DefaultCatalog: String = "geomesa-catalog"
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
  private val LayerViewReader = Derivation.Successful(ConfigReader.fromCursor(readLayerViewConfig))
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
  val DefaultZkPath: String = "geomesa/ds/kafka"
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
  val DefaultCatalog: String = org.locationtech.geomesa.kafka.data.DefaultCatalog
  val DefaultZkPath: String = org.locationtech.geomesa.kafka.data.DefaultZkPath
<<<<<<< HEAD
>>>>>>> d40f742b4 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
=======
=======
  val DefaultCatalog: String = "geomesa-catalog"
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
=======
  private val LayerViewReader = Derivation.Successful(ConfigReader.fromCursor(readLayerViewConfig))
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

  val DefaultCatalog: String = "geomesa-catalog"
<<<<<<< HEAD
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
=======
=======
  private val LayerViewReader = Derivation.Successful(ConfigReader.fromCursor(readLayerViewConfig))
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
  val DefaultZkPath: String = "geomesa/ds/kafka"
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cf457d8543 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
  val DefaultCatalog: String = "geomesa-catalog"
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
  private val LayerViewReader = Derivation.Successful(ConfigReader.fromCursor(readLayerViewConfig))
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

  val DefaultCatalog: String = "geomesa-catalog"
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
  private val LayerViewReader = Derivation.Successful(ConfigReader.fromCursor(readLayerViewConfig))
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
  val DefaultZkPath: String = "geomesa/ds/kafka"
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 642891ac8f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
  val DefaultCatalog: String = org.locationtech.geomesa.kafka.data.DefaultCatalog
  val DefaultZkPath: String = org.locationtech.geomesa.kafka.data.DefaultZkPath
<<<<<<< HEAD
>>>>>>> d40f742b4 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
=======
=======
  val DefaultCatalog: String = "geomesa-catalog"
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
=======
  private val LayerViewReader = Derivation.Successful(ConfigReader.fromCursor(readLayerViewConfig))
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

  val DefaultCatalog: String = "geomesa-catalog"
<<<<<<< HEAD
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
=======
=======
  private val LayerViewReader = Derivation.Successful(ConfigReader.fromCursor(readLayerViewConfig))
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
  val DefaultZkPath: String = "geomesa/ds/kafka"
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 846e61d5c9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
  val DefaultZkPath: String = "geomesa/ds/kafka"
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b5a7ce4912 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
  val DefaultCatalog: String = "geomesa-catalog"
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
  private val LayerViewReader = Derivation.Successful(ConfigReader.fromCursor(readLayerViewConfig))
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

<<<<<<< HEAD
<<<<<<< HEAD
  val DefaultCatalog: String = "geomesa-catalog"
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
  private val LayerViewReader = Derivation.Successful(ConfigReader.fromCursor(readLayerViewConfig))
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
  val DefaultZkPath: String = "geomesa/ds/kafka"
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cd74249075 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
  val DefaultCatalog: String = org.locationtech.geomesa.kafka.data.DefaultCatalog
  val DefaultZkPath: String = org.locationtech.geomesa.kafka.data.DefaultZkPath
<<<<<<< HEAD
>>>>>>> d40f742b4 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
>>>>>>> 0dba605f9e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
=======
=======
=======
  val DefaultCatalog: String = "geomesa-catalog"
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
=======
  private val LayerViewReader = Derivation.Successful(ConfigReader.fromCursor(readLayerViewConfig))
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

  val DefaultCatalog: String = "geomesa-catalog"
<<<<<<< HEAD
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
=======
=======
  private val LayerViewReader = Derivation.Successful(ConfigReader.fromCursor(readLayerViewConfig))
  private val LayerViewClassTag = ClassTag[LayerViewConfig](classOf[LayerViewConfig])

>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
  val DefaultZkPath: String = "geomesa/ds/kafka"
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> cf457d8543 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c933324bff (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))

  override val DisplayName = "Kafka (GeoMesa)"
  override val Description = "Apache Kafka\u2122 distributed log"

  // note: these are consumer-oriented and don't include producer configs
  override val ParameterInfo: Array[GeoMesaParam[_ <: AnyRef]] =
    Array(
      KafkaDataStoreParams.Brokers,
      KafkaDataStoreParams.Catalog,
      KafkaDataStoreParams.Zookeepers,
      KafkaDataStoreParams.ZkPath,
      KafkaDataStoreParams.ConsumerCount,
      KafkaDataStoreParams.ConsumerGroupPrefix,
      KafkaDataStoreParams.ConsumerConfig,
      KafkaDataStoreParams.ConsumerReadBack,
      KafkaDataStoreParams.CacheExpiry,
      KafkaDataStoreParams.DynamicCacheExpiry,
      KafkaDataStoreParams.EventTime,
      KafkaDataStoreParams.SerializationType,
      KafkaDataStoreParams.CqEngineIndices,
      KafkaDataStoreParams.IndexResolutionX,
      KafkaDataStoreParams.IndexResolutionY,
      KafkaDataStoreParams.IndexTiers,
      KafkaDataStoreParams.EventTimeOrdering,
      KafkaDataStoreParams.LazyLoad,
      KafkaDataStoreParams.LazyFeatures,
      KafkaDataStoreParams.LayerViews,
      KafkaDataStoreParams.MetricsReporters,
      KafkaDataStoreParams.AuditQueries,
      KafkaDataStoreParams.LooseBBox,
      KafkaDataStoreParams.Authorizations
    )

  override def canProcess(params: java.util.Map[String, _]): Boolean = {
    KafkaDataStoreParams.Brokers.exists(params) &&
        !params.containsKey("kafka.schema.registry.url") // defer to confluent data store
  }

  def buildConfig(params: java.util.Map[String, _]): KafkaDataStoreConfig = {
    import KafkaDataStoreParams._

    val brokers = checkBrokerPorts(Brokers.lookup(params))
    val zookeepers = Zookeepers.lookupOpt(params)
    val catalog = if (zookeepers.isEmpty) { createCatalogTopic(params) } else { createZkNamespace(params) }

    val topics = TopicConfig(TopicPartitions.lookup(params).intValue(), TopicReplication.lookup(params).intValue())

    val consumers = {
      val count = ConsumerCount.lookup(params).intValue
      val prefix = ConsumerGroupPrefix.lookupOpt(params) match {
        case None => ""
        case Some(p) if p.endsWith("-") => p
        case Some(p) => s"$p-"
      }
      val props = ConsumerConfig.lookupOpt(params).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
      val readBack = ConsumerReadBack.lookupOpt(params)
      KafkaDataStore.ConsumerConfig(count, prefix, props, readBack)
    }

    val producers = {
      val props = ProducerConfig.lookupOpt(params).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
      KafkaDataStore.ProducerConfig(props)
    }
    val clearOnStart = ClearOnStart.lookup(params)

    val serialization = SerializationTypes.fromName(SerializationType.lookup(params))

    val indices = {
      val cqEngine = {
        CqEngineIndices.lookupOpt(params) match {
          case Some(attributes) =>
            attributes.split(",").toSeq.map { attribute =>
              try {
                val Array(name, indexType) = attribute.split(":", 2)
                (name, CQIndexType.withName(indexType))
              } catch {
                case _: MatchError => throw new IllegalArgumentException(s"Invalid CQEngine index value: $attribute")
              }
            }

          case None =>
            // noinspection ScalaDeprecation
            if (!CqEngineCache.lookup(params).booleanValue()) { Seq.empty } else {
              logger.warn(s"Parameter '${CqEngineCache.key}' is deprecated, please use '${CqEngineIndices.key}' instead")
              Seq(KafkaDataStore.CqIndexFlag) // marker to trigger the cq engine index, will use config from the sft
            }

        }
      }
      val buckets = IndexResolution(IndexResolutionX.lookup(params), IndexResolutionY.lookup(params))
      val ssiTiers = parseSsiTiers(params)
      val lazyDeserialization = LazyFeatures.lookup(params).booleanValue()

      val expiry = {
        val simple = CacheExpiry.lookupOpt(params)
        val advanced = parseDynamicExpiry(params)
        val eventTime = EventTime.lookupOpt(params)
        val ordered = eventTime.isDefined && EventTimeOrdering.lookup(params).booleanValue()
        if (advanced.isEmpty) {
          simple.filter(_.isFinite) match {
            case None => NeverExpireConfig
            case Some(e) if e.length == 0 => ImmediatelyExpireConfig
            case Some(e) => eventTime.map(EventTimeConfig(e, _, ordered)).getOrElse(IngestTimeConfig(e))
          }
        } else {
          // INCLUDE has already been validated to be the last element (if present) in parseDynamicExpiry
          val withDefault = if (advanced.last._1.equalsIgnoreCase("INCLUDE")) { advanced } else {
            advanced :+ ("INCLUDE" -> simple.getOrElse(Duration.Inf)) // add at the end
          }
          val configs = eventTime match {
            case None => withDefault.map { case (f, e) => f -> IngestTimeConfig(e) }
            case Some(ev) => withDefault.map { case (f, e) => f -> EventTimeConfig(e, ev, ordered) }
          }
          FilteredExpiryConfig(configs)
        }
      }

      val executor = ExecutorTicker.lookupOpt(params)

      IndexConfig(expiry, buckets, ssiTiers, cqEngine, lazyDeserialization, executor)
    }

    val looseBBox = LooseBBox.lookup(params).booleanValue()

    val audit = if (!AuditQueries.lookup(params)) { None } else {
      Some((AuditLogger, buildAuditProvider(params), "kafka"))
    }
    val authProvider = buildAuthProvider(params)

    val layerViews = parseLayerViewConfig(params)

    val metrics = MetricsReporters.lookupOpt(params).map { conf =>
      val config = ConfigFactory.parseString(conf).resolve()
      val reporters =
        if (config.hasPath("reporters")) { config.getConfigList("reporters").asScala } else { Seq(config) }
      GeoMesaMetrics(catalog, reporters.toSeq)
    }

    val ns = Option(NamespaceParam.lookUp(params).asInstanceOf[String])

    // noinspection ScalaDeprecation
    Seq(CacheCleanup, CacheConsistency, CacheTicker).foreach { p =>
      if (params.containsKey(p.key)) {
        logger.warn(s"Parameter '${p.key}' is deprecated, and no longer has any effect")
      }
    }

    KafkaDataStoreConfig(catalog, brokers, zookeepers, consumers, producers, clearOnStart, topics, serialization,
      indices, looseBBox, layerViews, authProvider, audit, metrics, ns)
  }

  def buildSerializer(params: java.util.Map[String, _]): GeoMessageSerializerFactory = {
    val serialization = SerializationType.lookup(params)
    val serializationType = KafkaDataStoreParams.SerializationTypes.fromName(serialization)
    val nativeOpts = KafkaDataStoreParams.SerializationTypes.opts(serialization)
    val lazyOpts = if (LazyFeatures.lookup(params).booleanValue()) { Set(SerializationOption.Lazy) } else { Set.empty }
    new GeoMessageSerializerFactory(serializationType, nativeOpts ++ lazyOpts)
<<<<<<< HEAD
  }

  private def buildAuthProvider(params: java.util.Map[String, _]): AuthorizationsProvider = {
    import KafkaDataStoreParams.Authorizations
    // get the auth params passed in as a comma-delimited string
    val auths = Authorizations.lookupOpt(params).map(_.split(",").filterNot(_.isEmpty).toSeq).getOrElse(Seq.empty)
    AuthUtils.getProvider(params, auths)
  }

=======
  }

  private def buildAuthProvider(params: java.util.Map[String, _]): AuthorizationsProvider = {
    import KafkaDataStoreParams.Authorizations
    // get the auth params passed in as a comma-delimited string
    val auths = Authorizations.lookupOpt(params).map(_.split(",").filterNot(_.isEmpty)).getOrElse(Array.empty)
    security.getAuthorizationsProvider(params, auths)
  }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
  private def buildAuditProvider(params: java.util.Map[String, _]): AuditProvider =
    Option(AuditProvider.Loader.load(params)).getOrElse(NoOpAuditProvider)

  /**
    * Parse SSI tiers from parameters
    *
    * @param params params
    * @return
    */
  private[data] def parseSsiTiers(params: java.util.Map[String, _]): Seq[(Double, Double)] = {
    def parse(tiers: String): Option[Seq[(Double, Double)]] = {
      try {
        val parsed = tiers.split(",").map { xy =>
          val Array(x, y) = xy.split(":")
          (x.toDouble, y.toDouble)
        }
        Some(parsed.toSeq.sorted)
      } catch {
        case NonFatal(e) => logger.warn(s"Ignoring invalid index tiers '$tiers': ${e.toString}"); None
      }
    }

    KafkaDataStoreParams.IndexTiers.lookupOpt(params).flatMap(parse).getOrElse(SizeSeparatedBucketIndex.DefaultTiers)
  }

  /**
   * Parse the dynamic expiry param value into a seq of pairs
   *
   * @param params data store params
   * @return
   */
  private[data] def parseDynamicExpiry(params: java.util.Map[String, _]): Seq[(String, Duration)] = {
    lazy val key = s"Invalid property for parameter '${KafkaDataStoreParams.DynamicCacheExpiry.key}'"
    val expiry = KafkaDataStoreParams.DynamicCacheExpiry.lookupOpt(params).toSeq.flatMap { value =>
      ConfigFactory.parseString(value).resolve().root().unwrapped().asScala.toSeq.map {
        case (filter, exp: String) =>
          // validate the filter, but leave it as a string so we can optimize it based on the sft later
          try { ECQL.toFilter(filter) } catch {
            case NonFatal(e) => throw new IOException(s"$key, expected a CQL filter but got: $filter", e)
          }
          val duration = try { Duration(exp) } catch {
            case NonFatal(e) => throw new IOException(s"$key, expected a duration for key '$filter' but got: $exp", e)
          }
          filter -> duration

        case (filter, exp) =>
          throw new IOException(s"$key, expected a JSON string for key '$filter' but got: $exp")
      }
    }
    if (expiry.dropRight(1).exists(_._1.equalsIgnoreCase("INCLUDE"))) {
      throw new IOException(s"$key, defined a filter after Filter.INCLUDE (which would never be invoked)")
    }
    expiry
  }

  /**
   * Parse the typesafe config for a layer view. Views take the form:
   *
   * {
   *   foo-sft = [
   *     {
   *       type-name = foo-sft-enhanced
   *       filter = "foo = bar"
   *       transform = [ "foo", "bar", "baz", "blu" ]
   *     },
   *     {
   *       type-name = foo-sft-reduced
   *       filter = "foo = baz"
   *       transform = [ "foo", "bar", "baz" ]
   *     }
   *   ]
   * }
   *
   * @param params params
   * @return
   */
  private[kafka] def parseLayerViewConfig(params: java.util.Map[String, _]): Map[String, Seq[LayerViewConfig]] = {
    def asConfigObject(o: AnyRef) = o match {
      case c: ConfigObject => c
      case _ => throw new IllegalArgumentException(s"Invalid layer view, expected a config object but got: $o")
    }

    KafkaDataStoreParams.LayerViews.lookupOpt(params) match {
      case None => Map.empty[String, Seq[LayerViewConfig]]
      case Some(conf) =>
        val config = ConfigFactory.parseString(conf).resolve()
        val entries = config.entrySet().asScala.map { e =>
          val views = e.getValue match {
            case c: ConfigList => c.asScala.map(asConfigObject)
            case c => Seq(asConfigObject(c))
          }
          e.getKey -> views.map { c =>
<<<<<<< HEAD
            ConfigSource.fromConfig(c.toConfig).loadOrThrow[LayerViewConfig](LayerViewClassTag, LayerViewReader)
<<<<<<< HEAD
          }
        }
        val configs = entries.map(f => (f._1, f._2.toSeq))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5a7ce4912 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd74249075 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
=======
>>>>>>> 642891ac8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5a7ce4912 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd74249075 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
            pureconfig.loadConfigOrThrow[LayerViewConfig](c.toConfig)(LayerViewClassTag, LayerViewReader)
          }
        }
        val configs = entries.toMap
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5a7ce4912 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd74249075 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 515355cfcd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 515355cfcd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
          }
        }
        val configs = entries.map(f => (f._1, f._2.toSeq))
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 642891ac8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5a7ce4912 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> cd74249075 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 515355cfcd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
        val typeNames = configs.toSeq.flatMap(_._2.map(_.typeName))
        if (typeNames != typeNames.distinct) {
          throw new IllegalArgumentException(
            s"Detected duplicate type name in layer view config: ${config.root().render(ConfigRenderOptions.concise)}")
        }
<<<<<<< HEAD
        configs.toMap
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5a7ce4912 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd74249075 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
        configs
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        configs
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        configs
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5a7ce4912 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
        configs
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
        configs
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
=======
        configs
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 642891ac8f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5a7ce4912 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
        configs
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cd74249075 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 515355cfcd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
    }
  }

  /**
   * Parse a single layer view config
   *
   * @param cur cursor
   * @return
   */
  private def readLayerViewConfig(cur: ConfigCursor): Either[ConfigReaderFailures, LayerViewConfig] = {
    val config = for {
      obj       <- cur.asObjectCursor.right
      typeName  <- obj.atKey("type-name").right.flatMap(_.asString).right
      filter    <- readFilter(obj.atKeyOrUndefined("filter")).right
      transform <- readTransform(obj.atKeyOrUndefined("transform")).right
    } yield {
      LayerViewConfig(typeName, filter, transform)
    }
    config.right.flatMap { c =>
      if (c.filter.isEmpty && c.transform.isEmpty) {
        val err = "LayerViews must define at least one of 'filter' or 'transform'"
        cur.failed(new FailureReason { override def description: String = err })
      } else {
        Right(c)
      }
    }
  }

  private def readFilter(cur: ConfigCursor): Either[ConfigReaderFailures, Option[Filter]] = {
    if (cur.isUndefined) { Right(None) } else {
      cur.asString.right.flatMap { ecql =>
        try { Right(Some(ECQL.toFilter(ecql)).filter(_ != Filter.INCLUDE)) } catch {
          case NonFatal(e) => cur.failed(CannotConvert(ecql, "Filter", e.toString))
        }
      }
    }
  }

  private def readTransform(cur: ConfigCursor): Either[ConfigReaderFailures, Option[Seq[String]]] = {
    if (cur.isUndefined) { Right(None) } else {
      val transforms = cur.asList.right.flatMap { list =>
        list.foldLeft[Either[ConfigReaderFailures, Seq[String]]](Right(Seq.empty)) { case (res, elem) =>
          res.right.flatMap(r => elem.asString.right.map(r :+ _))
        }
      }
      transforms.right.map(t => if (t.isEmpty) { None } else { Some(t) })
    }
  }

  /**
<<<<<<< HEAD
   * Gets the catalog parameter - trims, removes leading/trailing "/" if needed
   *
   * @param params data store params
   * @return
   */
  private[data] def createCatalogTopic(params: java.util.Map[String, _]): String = {
    KafkaDataStoreParams.Catalog.lookupOpt(params)
        .map(p => StringUtils.strip(p, " /").replace("/", "-"))
        .filterNot(_.isEmpty)
        .getOrElse(DefaultCatalog)
  }

  /**
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5a7ce4912 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd74249075 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5a7ce4912 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 642891ac8f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5a7ce4912 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cd74249075 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 515355cfcd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e2420db68f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 291f0fa5ea (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a6cc128e9 (GEOMESA-3100 Kafka layer views (#2784))
    * Gets up a zk path parameter - trims, removes leading/trailing "/" if needed
    *
    * @param params data store params
    * @return
    */
  private[data] def createZkNamespace(params: java.util.Map[String, _]): String = {
    KafkaDataStoreParams.ZkPath.lookupOpt(params)
        .map(_.trim)
        .filterNot(_.isEmpty)
        .map(p => if (p.startsWith("/")) { p.substring(1).trim } else { p })  // leading '/'
        .map(p => if (p.endsWith("/")) { p.substring(0, p.length - 1).trim } else { p })  // trailing '/'
        .filterNot(_.isEmpty)
        .getOrElse(DefaultZkPath)
  }

  private def checkBrokerPorts(brokers: String): String = {
    if (brokers.indexOf(':') != -1) { brokers } else {
      try { brokers.split(",").map(b => s"${b.trim}:9092").mkString(",") } catch {
        case NonFatal(_) => brokers
      }
    }
  }
}
