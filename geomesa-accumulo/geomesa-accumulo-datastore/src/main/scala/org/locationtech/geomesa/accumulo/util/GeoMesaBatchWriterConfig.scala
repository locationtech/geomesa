/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.BatchWriterConfig
import org.locationtech.geomesa.accumulo.AccumuloProperties
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.text.Suffixes

import scala.util.Try

object GeoMesaBatchWriterConfig extends LazyLogging {

  protected[util] def fetchProperty(prop: SystemProperty): Option[Long] =
    for { p <- prop.option; num <- Try(java.lang.Long.parseLong(p)).toOption } yield num

  protected[util] def fetchMemoryProperty(prop: SystemProperty): Option[Long] =
    for { p <- prop.option; num <- Suffixes.Memory.bytes(p) } yield num

  protected[util] def buildBWC: BatchWriterConfig = {
    import AccumuloProperties.BatchWriterProperties

    val bwc = new BatchWriterConfig

    val latency = fetchProperty(BatchWriterProperties.WRITER_LATENCY_MILLIS).getOrElse(BatchWriterProperties.WRITER_LATENCY_MILLIS.default.toLong)
    logger.trace(s"GeoMesaBatchWriter config: maxLatency set to $latency milliseconds.")
    bwc.setMaxLatency(latency, TimeUnit.MILLISECONDS)


    val memory = fetchMemoryProperty(BatchWriterProperties.WRITER_MEMORY_BYTES).getOrElse(BatchWriterProperties.WRITER_MEMORY_BYTES.default.toLong)
    logger.trace(s"GeoMesaBatchWriter config: maxMemory set to $memory bytes.")
    bwc.setMaxMemory(memory)


    val threads = fetchProperty(BatchWriterProperties.WRITER_THREADS).map(_.toInt).getOrElse(BatchWriterProperties.WRITER_THREADS.default.toInt)
    logger.trace(s"GeoMesaBatchWriter config: maxWriteThreads set to $threads.")
    bwc.setMaxWriteThreads(threads)


    fetchProperty(BatchWriterProperties.WRITE_TIMEOUT_MILLIS).foreach { timeout =>
      logger.trace(s"GeoMesaBatchWriter config: maxTimeout set to $timeout seconds.")
      bwc.setTimeout(timeout, TimeUnit.MILLISECONDS)
    }

    bwc
  }

  def apply(): BatchWriterConfig = buildBWC
}
