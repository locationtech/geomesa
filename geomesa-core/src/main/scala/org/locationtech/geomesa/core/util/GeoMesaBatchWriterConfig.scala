package org.locationtech.geomesa.core.util

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.BatchWriterConfig

import scala.util.Try

object GeoMesaBatchWriterConfig extends Logging {
  val WRITERLATENCYSECONDS = "geomesa.batchwriter.latency.seconds"  // Measured in millis
  val WRITERLATENCYMILLIS  = "geomesa.batchwriter.latency.millis"   // Measured in millis
  val WRITERMEMORY         = "geomesa.batchwriter.memory"           // Measured in bytes
  val WRITERTHREADS        = "geomesa.batchwriter.maxthreads"
  val WRITETIMEOUT         = "geomesa.batchwriter.timeout.seconds"  // Timeout measured in seconds.  Likely unnecessary.

  val DEFAULTLATENCY   = 10000l   // 10 seconds
  val DEFAULTMAXMEMORY = 1000000l // 1 megabyte
  val DEFAULTTHREADS   = 10

  protected [util] def fetchProperty(prop: String): Option[Long] =
    for {
      p <- Option(System.getProperty(prop))
      l <- Try(java.lang.Long.parseLong(p)).toOption
    } yield l

  protected [util] def buildBWC: BatchWriterConfig = {
    val bwc = new BatchWriterConfig

    fetchProperty(WRITERLATENCYSECONDS) match {
      case Some(latency) =>
        logger.trace(s"GeoMesaBatchWriter config: maxLatency set to $latency seconds.")
        bwc.setMaxLatency(latency, TimeUnit.SECONDS)
      case None =>
        val milliLatency = fetchProperty(WRITERLATENCYMILLIS).getOrElse(DEFAULTLATENCY)
        logger.trace(s"GeoMesaBatchWriter config: maxLatency set to $milliLatency milliseconds.")
        bwc.setMaxLatency(milliLatency, TimeUnit.MILLISECONDS)
    }

    val memory = fetchProperty(WRITERMEMORY).getOrElse(DEFAULTMAXMEMORY)
    logger.trace(s"GeoMesaBatchWriter config: maxMemory set to $memory bytes.")
    bwc.setMaxMemory(memory)

    val threads = fetchProperty(WRITERTHREADS).map(_.toInt).getOrElse(DEFAULTTHREADS)
    logger.trace(s"GeoMesaBatchWriter config: maxWriteThreads set to $threads.")
    bwc.setMaxWriteThreads(threads.toInt)

    fetchProperty(WRITETIMEOUT).map { timeout =>
      logger.trace(s"GeoMesaBatchWriter config: maxTimeout set to $timeout seconds.")
      bwc.setTimeout(timeout, TimeUnit.SECONDS)
    }

    bwc
  }

  def apply(): BatchWriterConfig = buildBWC
}
