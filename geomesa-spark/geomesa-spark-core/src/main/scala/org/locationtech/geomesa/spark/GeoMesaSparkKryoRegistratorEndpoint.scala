/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.apache.spark.geomesa

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv, RpcTimeout}
import org.apache.spark.util.RpcUtils
import org.apache.spark.{SparkContext, SparkEnv}
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._
import org.opengis.feature.simple.SimpleFeatureType

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}


object GeoMesaSparkKryoRegistratorEndpoint extends LazyLogging {

  val EndpointName = "kryo-schema"

  private lazy val Timeout = RpcUtils.askRpcTimeout(SparkEnv.get.conf)
  private lazy val EndpointRef = RpcUtils.makeDriverRef(EndpointName, SparkEnv.get.conf, SparkEnv.get.rpcEnv)

  def init(): Unit = {
    Option(SparkEnv.get).foreach {
      sparkEnv =>
        sparkEnv.executorId match {
          case SparkContext.DRIVER_IDENTIFIER =>
            val rpcEnv = sparkEnv.rpcEnv
            Try(rpcEnv.setupEndpoint(EndpointName, new SchemaEndpoint(rpcEnv))) match {
              case Success(ref) => logger.info(s"kryo-schema rpc endpoint registered on driver ${ref.address}")
              // Can't test if endpoint already exists using available abstractions, failure expected if already bound
              case Failure(e) => logger.debug(s"kryo-schema rpc endpoint registration failed", e)
            }
          case _ =>
            logger.info(s"schemas request via rpc from ${EndpointRef.address}")
            val (result, delta) = askSync[Seq[(String, String)]](None)
            logger.info(s"schemas request via rpc, success (count=${result.size}) ($delta ms)")
            GeoMesaSparkKryoRegistrator.putTypes(result.map{case (name, spec) => createType(name, spec)})
      }
    }
  }

  class SchemaEndpoint(val rpcEnv: RpcEnv) extends RpcEndpoint with LazyLogging {
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case id: Int =>
        logger.info(s"schema get ($id) from ${context.senderAddress}")
        val spec = Option(GeoMesaSparkKryoRegistrator.getType(id)).map(sft => (sft.getTypeName, encodeType(sft)))
        context.reply(spec)
      case (name: String, spec: String) =>
        val id = GeoMesaSparkKryoRegistrator.putType(createType(name, spec))
        logger.info(s"schmea put ($id) from ${context.senderAddress}")
        context.reply(id)
      case None =>
        logger.info(s"schemas requested from ${context.senderAddress}")
        val specs = GeoMesaSparkKryoRegistrator.getTypes.map(sft => (sft.getTypeName, encodeType(sft)))
        context.reply(specs)
    }
  }

  private def askSync[T: ClassTag](message: Any, timeout: RpcTimeout = Timeout): (T, Long) = {
    val start = System.nanoTime()
    val result = timeout.awaitResult(EndpointRef.ask[T](message, timeout))
    val delta = (System.nanoTime() - start)/1000000L
    (result, delta)
  }

  lazy val getType: Int => Option[SimpleFeatureType] =
    Option(SparkEnv.get)
      .filterNot(_.executorId == SparkContext.DRIVER_IDENTIFIER)
      .map(_ => getTypeExecutor).getOrElse(getTypeNoOp)

  private val getTypeExecutor: Int => Option[SimpleFeatureType] = id => {
    logger.info(s"schema $id get via rpc from ${EndpointRef.address}")
    val (result, delta) = askSync[Option[(String, String)]](id)
    result match {
      case Some((name, spec)) =>
        logger.info(s"schema $id get via rpc, success ($delta ms)")
        Option(createType(name, spec))
      case _ =>
        logger.warn(s"schema $id get via rpc, failed ($delta ms)")
        None
    }
  }

  private val getTypeNoOp: Int => Option[SimpleFeatureType] = _ => None

  lazy val putType: (Int, SimpleFeatureType) => Unit = Option(SparkEnv.get)
    .filterNot(_.executorId == SparkContext.DRIVER_IDENTIFIER)
    .map(_ => putTypeExecutor).getOrElse(putTypeNoOp)

  private val putTypeExecutor: (Int, SimpleFeatureType) => Unit = (id, sft) => {
    logger.info(s"schema ${sft.getTypeName} put via rpc to ${EndpointRef.address}")
    val (result, delta) = askSync[Int]((sft.getTypeName, encodeType(sft)))
    result match {
      case _ :Int => logger.info(s"schema $id put via rpc, success ($delta ms)")
      case _ => logger.warn(s"schema $id put via rpc, failed ($delta ms)")
    }
  }

  private val putTypeNoOp: (Int, SimpleFeatureType) => Unit = (_,_) => {}

}
