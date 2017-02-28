/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.apache.spark.geomesa

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}
import org.apache.spark.util.RpcUtils
import org.apache.spark.{SparkContext, SparkEnv}
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.{Failure, Success, Try}


object GeoMesaSparkKryoRegistratorEndpoint extends LazyLogging {

  val EndpointName = "kryo-schema"

  private lazy val Timeout = RpcUtils.askRpcTimeout(SparkEnv.get.conf)
  private lazy val EndpointRef = RpcUtils.makeDriverRef(EndpointName, SparkEnv.get.conf, SparkEnv.get.rpcEnv)

  def register(): Unit = {
    Option(SparkEnv.get)
      .filter(_.executorId == SparkContext.DRIVER_IDENTIFIER)
      .map(_.rpcEnv)
      .foreach { rpcEnv =>
        logger.info("setting up kryo-schema rpc endpoint on driver")
        Try(rpcEnv.setupEndpoint(EndpointName, new SchemaEndpoint(rpcEnv))) match {
          case Success(endpointRef) => logger.info(s"kryo-schema rpc endpoint registered on driver")
          case Failure(e) => logger.debug(s"kryo-schema rpc endpoint registration failed", e)
        }
      }
  }

  class SchemaEndpoint(val rpcEnv: RpcEnv) extends RpcEndpoint with LazyLogging {
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case id: Int =>
        logger.info(s"request for kryo schema ${id} from ${context.senderAddress}")
        val spec = Option(GeoMesaSparkKryoRegistrator.getType(id)).map(sft => (sft.getTypeName, encodeType(sft)))
        context.reply(spec)
    }
  }

  lazy val resolver: Int => Option[SimpleFeatureType] =
    Option(SparkEnv.get)
      .filterNot(_.executorId == SparkContext.DRIVER_IDENTIFIER)
      .map(_ => executorResolver).getOrElse(noopResolver)

  private val executorResolver: Int => Option[SimpleFeatureType] = id => {
    logger.info(s"schema $id request via rpc from ${EndpointRef.address}")
    val ask = EndpointRef.ask[Option[(String, String)]](id)
    Timeout.awaitResult(ask) match {
      case Some((name, spec)) =>
        logger.info(s"schema $id resolved via rpc")
        Option(createType(name, spec))
      case _ =>
        logger.warn(s"schema $id not resolved via rpc")
        None
    }
  }

  private val noopResolver: Int => Option[SimpleFeatureType] = _ => None
}
