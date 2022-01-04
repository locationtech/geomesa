/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.consumer

import java.io.Closeable
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.Consumer
import org.locationtech.geomesa.kafka.consumer.ThreadedConsumer.{ConsumerErrorHandler, LoggingConsumerErrorHandler}

abstract class BaseThreadedConsumer(consumers: Seq[Consumer[Array[Byte], Array[Byte]]])
    extends Closeable with LazyLogging {

  import scala.collection.JavaConverters._

  @volatile
  private var open = true

  private val executor: ExecutorService = Executors.newFixedThreadPool(consumers.length)

  def startConsumers(handler: Option[ConsumerErrorHandler] = None): Unit = {
    val format = if (consumers.lengthCompare(10) > 0) { "%02d" } else { "%d" }
    val topics = consumers.flatMap(_.subscription().asScala).distinct
    val h = handler.getOrElse(new LoggingConsumerErrorHandler(logger, topics))
    var i = 0
    consumers.foreach { c =>
      executor.execute(createConsumerRunnable(String.format(format, Int.box(i)), c, h))
      i += 1
    }
    logger.debug(s"Started $i consumer(s) on topic ${topics.mkString(", ")}")
  }

  override def close(): Unit = {
    open = false
    executor.shutdown()
    executor.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
  }

  protected def isOpen: Boolean = open

  protected def createConsumerRunnable(
      id: String,
      consumer: Consumer[Array[Byte], Array[Byte]],
      handler: ConsumerErrorHandler): Runnable
}
