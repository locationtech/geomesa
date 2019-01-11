/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.geomesa

import java.io.Serializable

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

  val EnablePropertyKey = "spark.geomesa.kryo.rpc.enable"

  val EndpointName = "kryo-schema"

  private lazy val Timeout = RpcUtils.askRpcTimeout(SparkEnv.get.conf)
  private lazy val EndpointRef = RpcUtils.makeDriverRef(EndpointName, SparkEnv.get.conf, SparkEnv.get.rpcEnv)

  lazy val Client: KryoClient = Option(SparkEnv.get)
    .filterNot(_.executorId == SparkContext.DRIVER_IDENTIFIER)
    .filter(endpointEnabled)
    .map(_ => ExecutorKryoClient).getOrElse(NoOpKryoClient)

  private def endpointEnabled(sparkEnv: SparkEnv) =
    !sparkEnv.conf.get(EnablePropertyKey, "true").equalsIgnoreCase("false")

  def init(): Unit = {
    Option(SparkEnv.get).foreach {
      sparkEnv =>
        if (endpointEnabled(sparkEnv)) {
          sparkEnv.executorId match {
            case SparkContext.DRIVER_IDENTIFIER =>
              val rpcEnv = sparkEnv.rpcEnv
              Try(rpcEnv.setupEndpoint(EndpointName, new KryoEndpoint(rpcEnv))) match {
                case Success(ref) =>
                  logger.info(s"$EndpointName rpc endpoint registered on driver ${ref.address}")
                case Failure(e: IllegalArgumentException) =>
                  logger.debug(s"$EndpointName rpc endpoint registration failed, may have been already registered", e)
                case Failure(e: Exception) =>
                  logger.warn(s"$EndpointName rpc endpoint registration failed", e)
              }
            case _ => GeoMesaSparkKryoRegistrator.putTypes(Client.getTypes())
          }
        } else {
          logger.debug(s"$EndpointName rpc endpoint disabled")
        }
    }
  }

  class KryoEndpoint(val rpcEnv: RpcEnv) extends RpcEndpoint {
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case message: KryoMessage[_] =>
        logger.info(s"$message received via rpc from ${context.senderAddress}")
        context.reply(message.reply)
    }
  }

  trait KryoMessage[R] {
    def reply: R
    def ask(timeout: RpcTimeout = Timeout)(implicit c: ClassTag[R]): R = {
      logger.info(s"$this sent via rpc to ${EndpointRef.address}")
      val start = System.nanoTime()
      val result = timeout.awaitResult[R](EndpointRef.ask[R](this, timeout))
      val delta = (System.nanoTime() - start) / 1000000L
      logger.info(s"$this response via rpc, $delta ms")
      result
    }
  }

  class KryoGetTypeMessage(id: Int) extends KryoMessage[Option[(String, String)]] with Serializable {
    def reply: Option[(String, String)] = Option(GeoMesaSparkKryoRegistrator.getType(id))
    override def toString: String = s"getType(id=$id)"
  }

  class KryoGetTypesMessage() extends KryoMessage[Seq[(String, String)]] with Serializable {
    def reply: Seq[(String, String)] = GeoMesaSparkKryoRegistrator.getTypes.map(encodeSchema)
    override def toString: String = s"getTypes()"
  }

  class KryoPutTypeMessage(id: Int, name: String, spec: String) extends KryoMessage[Int] with Serializable {
    def this(id: Int, sft: SimpleFeatureType) = this(id, sft.getTypeName, encodeType(sft))
    def reply: Int = GeoMesaSparkKryoRegistrator.putType(createType(name, spec))
    override def toString: String = s"putType(id=$id, name=$name, spec=...)"
  }

  trait KryoClient {
    def getTypes(): Seq[SimpleFeatureType]
    def getType(id: Int): Option[SimpleFeatureType]
    def putType(id: Int, schema: SimpleFeatureType): Int
  }

  protected object ExecutorKryoClient extends KryoClient {
    def getTypes(): Seq[SimpleFeatureType] = new KryoGetTypesMessage().ask().map(decodeSchema)
    def getType(id: Int): Option[SimpleFeatureType] = new KryoGetTypeMessage(id).ask()
    def putType(id: Int, sft: SimpleFeatureType): Int = new KryoPutTypeMessage(id, sft).ask()
  }

  protected object NoOpKryoClient extends KryoClient {
    def getTypes(): Seq[SimpleFeatureType] = Seq.empty
    def getType(id: Int) = None
    def putType(id: Int, sft: SimpleFeatureType): Int = id
  }

  implicit def encodeSchema(t: SimpleFeatureType): (String, String) = (t.getTypeName, encodeType(t))
  implicit def decodeSchema(t: (String, String)): SimpleFeatureType = createType(t._1, t._2)
  implicit def optionSchema(t: Option[(String, String)]): Option[SimpleFeatureType] = t.map(decodeSchema)

}
