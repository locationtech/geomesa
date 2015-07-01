/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.accumulo.util

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.BatchWriterConfig
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties.PropAndDefault

import scala.util.Try

object GeoMesaBatchWriterConfig extends Logging {

  protected[util] def fetchProperty(prop: PropAndDefault): Option[Long] =
    for { p <- Option(prop.get); num <- Try(java.lang.Long.parseLong(p)).toOption } yield num

  protected[util] def fetchMemoryProperty(prop: PropAndDefault): Option[Long] =
    for { p <- Option(prop.get); num <- parseMemoryProperty(p) } yield num

  protected[util] def parseMemoryProperty(prop: String): Option[Long] = {

    //Scala regex matches the whole string, the leading ^ and trailing $ is implied.
    //First group matches numbers, second group must correspond to suffixMap keys below
    val matchSuffix = """(\d+)([KkMmGg])?""".r

    //Define suffixes based on powers of 2
    val suffixMap = Map(
      "K" -> 1024l,
      "k" -> 1024l,
      "M" -> 1024l * 1024l,
      "m" -> 1024l * 1024l,
      "G" -> 1024l * 1024l * 1024l,
      "g" -> 1024l * 1024l * 1024l
    )

    prop match {
      case matchSuffix(number, null) =>
        //For-comprehension here also matches Scala.Long and Java.Long types
        for {
          num <- Try(java.lang.Long.parseLong(number)).toOption
        } yield num
      case matchSuffix(number, suffix) if suffixMap.contains(suffix) =>
        for {
        //Because we are not using parseLong(), we need to check for overflow
          num <- Try(java.lang.Long.valueOf(number.toLong * suffixMap(suffix))).filter(_ > 0).toOption
        } yield num
      case _ => None
    }
  }

  protected[util] def buildBWC: BatchWriterConfig = {
    import GeomesaSystemProperties.BatchWriterProperties

    val bwc = new BatchWriterConfig

    val latency = fetchProperty(BatchWriterProperties.WRITER_LATENCY_MILLIS)
        .getOrElse(GeomesaSystemProperties.BatchWriterProperties.WRITER_LATENCY_MILLIS.default.toLong)
    logger.trace(s"GeoMesaBatchWriter config: maxLatency set to $latency milliseconds.")
    bwc.setMaxLatency(latency, TimeUnit.MILLISECONDS)

    val memory = fetchMemoryProperty(BatchWriterProperties.WRITER_MEMORY_BYTES)
        .getOrElse(BatchWriterProperties.WRITER_MEMORY_BYTES.default.toLong)
    logger.trace(s"GeoMesaBatchWriter config: maxMemory set to $memory bytes.")
    bwc.setMaxMemory(memory)

    val threads = fetchProperty(BatchWriterProperties.WRITER_THREADS).map(_.toInt)
        .getOrElse(BatchWriterProperties.WRITER_THREADS.default.toInt)
    logger.trace(s"GeoMesaBatchWriter config: maxWriteThreads set to $threads.")
    bwc.setMaxWriteThreads(threads.toInt)

    fetchProperty(BatchWriterProperties.WRITE_TIMEOUT_MILLIS).foreach { timeout =>
      logger.trace(s"GeoMesaBatchWriter config: maxTimeout set to $timeout seconds.")
      bwc.setTimeout(timeout, TimeUnit.MILLISECONDS)
    }

    bwc
  }

  def apply(): BatchWriterConfig = buildBWC
}
