package org.locationtech.geomesa.accumulo.util

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.BatchWriterConfig

import scala.util.Try

object GeoMesaBatchWriterConfig extends Logging {
  val WRITER_LATENCY_SECONDS = "geomesa.batchwriter.latency.seconds"  // Measured in seconds
  val WRITER_LATENCY_MILLIS  = "geomesa.batchwriter.latency.millis"   // Measured in millis
  val WRITER_MEMORY          = "geomesa.batchwriter.memory"           // Measured in bytes
  val WRITER_THREADS         = "geomesa.batchwriter.maxthreads"
  val WRITE_TIMEOUT          = "geomesa.batchwriter.timeout.seconds"  // Timeout measured in seconds.  Likely unnecessary.

  val DEFAULT_LATENCY   = 10000l   // 10 seconds
  val DEFAULT_MAX_MEMORY = 1000000l // 1 megabyte
  val DEFAULT_THREADS   = 10

  protected [util] def fetchProperty(prop: String): Option[Long] =
    for {
      p <- Option(System.getProperty(prop))
      num <- Try(java.lang.Long.parseLong(p)).toOption
    } yield num

  protected [util] def buildBWC: BatchWriterConfig = {
    val bwc = new BatchWriterConfig

    fetchProperty(WRITER_LATENCY_SECONDS) match {
      case Some(latency) =>
        logger.trace(s"GeoMesaBatchWriter config: maxLatency set to $latency seconds.")
        bwc.setMaxLatency(latency, TimeUnit.SECONDS)
      case None =>
        val milliLatency = fetchProperty(WRITER_LATENCY_MILLIS).getOrElse(DEFAULT_LATENCY)
        logger.trace(s"GeoMesaBatchWriter config: maxLatency set to $milliLatency milliseconds.")
        bwc.setMaxLatency(milliLatency, TimeUnit.MILLISECONDS)
    }

    // TODO: Allow users to specify member with syntax like 100M or 50k.
    // https://geomesa.atlassian.net/browse/GEOMESA-735
    val memory = fetchProperty(WRITER_MEMORY).getOrElse(DEFAULT_MAX_MEMORY)
    logger.trace(s"GeoMesaBatchWriter config: maxMemory set to $memory bytes.")
    bwc.setMaxMemory(memory)

    val threads = fetchProperty(WRITER_THREADS).map(_.toInt).getOrElse(DEFAULT_THREADS)
    logger.trace(s"GeoMesaBatchWriter config: maxWriteThreads set to $threads.")
    bwc.setMaxWriteThreads(threads.toInt)

    fetchProperty(WRITE_TIMEOUT).foreach { timeout =>
      logger.trace(s"GeoMesaBatchWriter config: maxTimeout set to $timeout seconds.")
      bwc.setTimeout(timeout, TimeUnit.SECONDS)
    }

    bwc
  }

  def apply(): BatchWriterConfig = buildBWC
}
