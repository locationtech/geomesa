/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStoreFactory, AccumuloDataStoreParams}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{GeoMesaDataStoreInfo, GeoMesaDataStoreParams}
import org.locationtech.geomesa.security.SecurityParams
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

import java.awt.RenderingHints.Key
import java.time.Clock
import scala.reflect.ClassTag

class LambdaDataStoreFactory extends DataStoreFactorySpi {

  import LambdaDataStoreParams.{ClockParam, NamespaceParam}

  override def createDataStore(params: java.util.Map[String, _]): DataStore = {
    // TODO GEOMESA-1891 attribute level vis
    val persistence = new AccumuloDataStoreFactory().createDataStore(LambdaDataStoreFactory.filter(params))
    val config = LambdaDataStoreParams.parse(params, persistence.config.catalog)
    val clock = ClockParam.lookupOpt(params).getOrElse(Clock.systemUTC())
    new LambdaDataStore(persistence, config)(clock)
  }

  override def createNewDataStore(params: java.util.Map[String, _]): DataStore = createDataStore(params)

  override def isAvailable: Boolean = true

  override def getDisplayName: String = LambdaDataStoreFactory.DisplayName

  override def getDescription: String = LambdaDataStoreFactory.Description

  override def getParametersInfo: Array[Param] = Array(LambdaDataStoreFactory.ParameterInfo :+ NamespaceParam: _*)

  override def canProcess(params: java.util.Map[String, _]): Boolean =
    LambdaDataStoreFactory.canProcess(params)

  override def getImplementationHints: java.util.Map[Key, _] = java.util.Collections.emptyMap()
}

object LambdaDataStoreFactory extends GeoMesaDataStoreInfo {

  import LambdaDataStoreParams._

  override val DisplayName = "Kafka/Accumulo Lambda (GeoMesa)"

  override val Description = "Hybrid store using Kafka for recent events and Accumulo for long-term storage"

  override val ParameterInfo: Array[GeoMesaParam[_ <: AnyRef]] =
    Array(
      Params.Accumulo.InstanceParam,
      Params.Accumulo.ZookeepersParam,
      Params.Accumulo.CatalogParam,
      Params.Accumulo.UserParam,
      Params.Accumulo.PasswordParam,
      Params.Accumulo.KeytabParam,
      BrokersParam,
      ZookeepersParam,
      ExpiryParam,
      PersistParam,
      AuthsParam,
      ForceEmptyAuthsParam,
      QueryTimeoutParam,
      QueryThreadsParam,
      Params.Accumulo.RecordThreadsParam,
      Params.Accumulo.WriteThreadsParam,
      PartitionsParam,
      ConsumersParam,
      ProducerOptsParam,
      ConsumerOptsParam,
      LooseBBoxParam,
      GenerateStatsParam,
      AuditQueriesParam
    )

  override def canProcess(params: java.util.Map[String, _]): Boolean =
    AccumuloDataStoreFactory.canProcess(LambdaDataStoreFactory.filter(params)) &&
        Seq(ExpiryParam, BrokersParam, ZookeepersParam).forall(_.exists(params))

  // noinspection TypeAnnotation
  object Params extends GeoMesaDataStoreParams with SecurityParams {

    object Accumulo {
      val InstanceParam      = copy(AccumuloDataStoreParams.InstanceNameParam)
      val ZookeepersParam    = copy(AccumuloDataStoreParams.ZookeepersParam)
      val UserParam          = copy(AccumuloDataStoreParams.UserParam)
      val PasswordParam      = copy(AccumuloDataStoreParams.PasswordParam)
      val KeytabParam        = copy(AccumuloDataStoreParams.KeytabPathParam)
      val RecordThreadsParam = copy(AccumuloDataStoreParams.RecordThreadsParam)
      val WriteThreadsParam  = copy(AccumuloDataStoreParams.WriteThreadsParam)
      val CatalogParam       = copy(AccumuloDataStoreParams.CatalogParam)
    }
  }

  private def copy[T <: AnyRef](p: GeoMesaParam[T])(implicit ct: ClassTag[T]): GeoMesaParam[T] = {
    new GeoMesaParam[T](s"lambda.${p.key}", p.description.toString, optional = !p.required, default = p.default,
      password = p.password, largeText = p.largeText, extension = p.extension, deprecatedKeys = p.deprecatedKeys,
      deprecatedParams = p.deprecatedParams, systemProperty = p.systemProperty)
  }

  private def filter(params: java.util.Map[String, _]): java.util.Map[String, _] = {
    // note: includes a bit of redirection to allow us to pass non-serializable values in to tests
    import scala.collection.JavaConverters._
    Map[String, Any](params.asScala.toSeq: _ *)
        .map { case (k, v) => (if (k.startsWith("lambda.")) { k.substring(7) } else { k }, v) }
        .asJava
  }
}
