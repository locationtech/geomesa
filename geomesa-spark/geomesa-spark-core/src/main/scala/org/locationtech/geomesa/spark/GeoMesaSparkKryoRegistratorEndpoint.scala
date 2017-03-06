/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

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

  val EndpointName = "kryo-schema"

  private lazy val Timeout = RpcUtils.askRpcTimeout(SparkEnv.get.conf)
  private lazy val EndpointRef = RpcUtils.makeDriverRef(EndpointName, SparkEnv.get.conf, SparkEnv.get.rpcEnv)

  lazy val Client: KryoClient = Option(SparkEnv.get)
    .filterNot(_.executorId == SparkContext.DRIVER_IDENTIFIER)
    .map(_ => ExecutorKryoClient).getOrElse(DriverKryoClient)

  def init(): Unit = {
    Option(SparkEnv.get).foreach {
      sparkEnv =>
        sparkEnv.executorId match {
          case SparkContext.DRIVER_IDENTIFIER =>
            val rpcEnv = sparkEnv.rpcEnv
            Try(rpcEnv.setupEndpoint(EndpointName, new KryoEndpoint(rpcEnv))) match {
              case Success(ref) => logger.info(s"$EndpointName rpc endpoint registered on driver ${ref.address}")
              // Can't test if endpoint already exists using available abstractions, failure expected if already bound
              case Failure(e) => logger.debug(s"$EndpointName rpc endpoint registration failed", e)
            }
          case _ => GeoMesaSparkKryoRegistrator.putTypes(Client.getTypes())
        }
    }
  }

  class KryoEndpoint(val rpcEnv: RpcEnv) extends RpcEndpoint with LazyLogging {
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

  class KryoPutTypeMessage(name: String, spec: String) extends KryoMessage[Int] with Serializable {
    def this(sft: SimpleFeatureType) = this(sft.getTypeName, encodeType(sft))

    def reply: Int = GeoMesaSparkKryoRegistrator.putType(createType(name, spec))

    override def toString: String = s"putType(...)"
  }

  trait KryoClient {
    def getTypes(): Seq[SimpleFeatureType]

    def getType(id: Int): Option[SimpleFeatureType]

    def putType(id: Int, schema: SimpleFeatureType): Int
  }

  protected object ExecutorKryoClient extends KryoClient {
    def getTypes(): Seq[SimpleFeatureType] = new KryoGetTypesMessage().ask().map(decodeSchema)

    def getType(id: Int): Option[SimpleFeatureType] = new KryoGetTypeMessage(id).ask()

    def putType(id: Int, sft: SimpleFeatureType): Int = new KryoPutTypeMessage(sft).ask()
  }

  protected object DriverKryoClient extends KryoClient {
    def getTypes(): Seq[SimpleFeatureType] = Seq.empty

    def getType(id: Int) = None

    def putType(id: Int, sft: SimpleFeatureType): Int = id
  }

  implicit def encodeSchema(t: SimpleFeatureType): (String, String) = (t.getTypeName, encodeType(t))

  implicit def decodeSchema(t: (String, String)): SimpleFeatureType = createType(t._1, t._2)

  implicit def optionSchema(t: Option[(String, String)]): Option[SimpleFeatureType] = t.map(decodeSchema)

}
