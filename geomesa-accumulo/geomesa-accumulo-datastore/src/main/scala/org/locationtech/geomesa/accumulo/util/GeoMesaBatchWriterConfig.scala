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

  val DEFAULT_LATENCY    = 10000l   // 10 seconds
  val DEFAULT_MAX_MEMORY = 1000000l // 1 megabyte
  val DEFAULT_THREADS    = 10

  protected [util] def fetchProperty(prop: String): Option[Long] =
    for {
      p <- Option(System.getProperty(prop))
      num <- Try(java.lang.Long.parseLong(p)).toOption
    } yield num

  protected [util] def fetchMemoryProperty(prop: String): Option[Long] =
    for {
      p <- Option(System.getProperty(prop))
      num <- parseMemoryProperty(p)
    } yield num

  protected [util] def parseMemoryProperty(prop: String): Option[Long] = {

    //Scala regex matches the whole string, the leading ^ and trailing $ is implied.
    val matchSuffix = """(\d+)([A-Za-z]+)?""".r

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
        for {
          num <- Try(java.lang.Long.parseLong(number)).toOption
        } yield num
      case matchSuffix(number, suffix) =>
        if(suffixMap.contains(suffix))
          for {
            num <- Try(java.lang.Long.valueOf(number.toLong * suffixMap(suffix))).toOption
          } yield num
        else None
      case _ => None
    }
  }

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

    val memory = fetchMemoryProperty(WRITER_MEMORY).getOrElse(DEFAULT_MAX_MEMORY)
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
