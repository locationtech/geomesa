/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.{File, FileInputStream, PrintStream}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.geotools.data.{DataStoreFinder, DataUtilities, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.joda.time.Period
import org.joda.time.format.PeriodFormatterBuilder
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.convert.Transformers.DefaultCounter
import org.locationtech.geomesa.utils.classpath.PathUtils
import org.locationtech.geomesa.utils.stats.CountingInputStream
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class ConverterIngest(dsParams: Map[String, String],
                      sft: SimpleFeatureType,
                      converterConfig: Config,
                      inputs: Seq[String],
                      numLocalThreads: Int)
    extends Runnable with LazyLogging {

  import org.locationtech.geomesa.tools.ingest.ConverterIngest._

  val ds = DataStoreFinder.getDataStore(dsParams)

  // (progress, start time, pass, fail, done)
  val statusCallback: (Float, Long, Long, Long, Boolean) => Unit =
    if (dsParams.get(AccumuloDataStoreParams.mockParam.getName).exists(_.toBoolean)) {
      val progress = printProgress(System.err, buildString('\u26AC', 60), ' ', _: Char) _
      var state = false
      (p, s, pass, fail, d) => {
        state = !state
        if (state) progress('\u15e7')(p, s, pass, fail, d) else progress('\u2b58')(p, s, pass, fail, d)
      }
    } else {
      printProgress(System.err, buildString(' ', 60), '\u003d', '\u003e')
    }

  override def run(): Unit = {
    // create schema for the feature prior to Ingest job
    logger.info(s"Creating schema ${sft.getTypeName}")
    ds.createSchema(sft)

    if (inputs.head.toLowerCase.startsWith("hdfs://")) {
      logger.info("Running ingestion in distributed mode")
      runDistributed()
    } else {
      logger.info("Running ingestion in local mode")
      runLocal()
    }
  }

  private def runLocal(): Unit = {

    // Global failure shared between threads
    val (written, failed) = (new AtomicLong(0), new AtomicLong(0))

    class LocalIngestCounter extends DefaultCounter {
      // keep track of failure at a global level, keep line counts and success local
      override def incFailure(i: Long): Unit = failed.getAndAdd(i)
      override def getFailure: Long          = failed.get()
    }

    val bytesRead = new AtomicLong(0L)

    class LocalIngestWorker(file: File) extends Runnable {
      override def run(): Unit = {
        try {
          val fw = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
          val converter = SimpleFeatureConverters.build(sft, converterConfig)
          val ec = converter.createEvaluationContext(Map("inputFilePath" -> file.getAbsolutePath), new LocalIngestCounter)
          // count the raw bytes read from the file, as that's what we based our total on
          val countingStream = new CountingInputStream(new FileInputStream(file))
          val is = PathUtils.handleCompression(countingStream, file.getPath)
          try {
            converter.process(is, ec).foreach { sf =>
              val toWrite = fw.next()
              toWrite.setAttributes(sf.getAttributes)
              toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
              toWrite.getUserData.putAll(sf.getUserData)
              toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
              try {
                fw.write()
                written.incrementAndGet()
              } catch {
                case e: Exception =>
                  logger.error(s"Failed to write '${DataUtilities.encodeFeature(toWrite)}'", e)
                  failed.incrementAndGet()
              }
              bytesRead.addAndGet(countingStream.getCount)
              countingStream.resetCount()
            }
          } finally {
            IOUtils.closeQuietly(is)
            IOUtils.closeQuietly(fw)
          }
        } catch {
          case e: Exception =>
            // Don't kill the entire program bc this thread was bad! use outer try/catch
            logger.error(s"Fatal error running local ingest worker on file ${file.getPath}", e)
        }
      }
    }

    val files = inputs.flatMap(PathUtils.interpretPath)
    val numFiles = files.length
    val totalLength = files.map(_.length).sum.toFloat

    def progress(): Float = bytesRead.get() / totalLength

    logger.info(s"Ingesting ${getPlural(numFiles, "file")} with ${getPlural(numLocalThreads, "thread")}")

    val start = System.currentTimeMillis()
    val es = Executors.newFixedThreadPool(numLocalThreads)
    files.foreach(f => es.submit(new LocalIngestWorker(f)))
    es.shutdown()

    while (!es.isTerminated) {
      Thread.sleep(1000)
      statusCallback(progress(), start, written.get(), failed.get(), false)
    }
    statusCallback(progress(), start, written.get(), failed.get(), true)

    logger.info(s"Local ingestion complete in ${getTime(start)}")
    logger.info(getStatInfo(written.get, failed.get))
  }

  private def runDistributed(): Unit = {
    val start = System.currentTimeMillis()
    val status = statusCallback(_: Float, start, _: Long, _: Long, _: Boolean)
    val (success, failed) = ConverterIngestJob.run(dsParams, sft, converterConfig, inputs, status)
    logger.info(s"Distributed ingestion complete in ${getTime(start)}")
    logger.info(getStatInfo(success, failed))
  }
}

object ConverterIngest {

  val PeriodFormatter =
    new PeriodFormatterBuilder().minimumPrintedDigits(2).printZeroAlways()
      .appendHours().appendSeparator(":").appendMinutes().appendSeparator(":").appendSeconds().toFormatter

  /**
   * Prints progress using the provided output stream. Progress will be overwritten using '\r', and will only
   * include a line feed if done == true
   */
  def printProgress(out: PrintStream,
                    emptyBar: String,
                    replacement: Char,
                    indicator: Char)(progress: Float, start: Long, pass: Long, fail: Long, done: Boolean): Unit = {
    val numFilled = (emptyBar.length * progress).toInt
    val bar = if (numFilled < 1) {
      emptyBar
    } else if (numFilled >= emptyBar.length) {
      buildString(replacement, numFilled)
    } else {
      s"${buildString(replacement, numFilled - 1)}$indicator${emptyBar.substring(numFilled)}"
    }
    val percent = f"${(progress * 100).toInt}%3d"
    // use \r to replace current line
    // trailing space separates cursor
    out.print(s"\r[$bar] $percent% complete $pass ingested $fail failed in ${getTime(start)} ")
    if (done) {
      out.println()
    }
  }

  private def buildString(c: Char, length: Int) = {
    val sb = new StringBuilder(length)
    (0 until length).foreach(_ => sb.append(c))
    sb.toString()
  }

  /**
   * Gets elapsed time as a string
   */
  def getTime(start: Long): String = PeriodFormatter.print(new Period(System.currentTimeMillis() - start))

  /**
   * Gets status as a string
   */
  def getStatInfo(successes: Long, failures: Long): String = {
    val failureString = if (failures == 0) {
      "with no failures"
    } else {
      s"and failed to ingest ${getPlural(failures, "feature")}"
    }
    s"Ingested ${getPlural(successes, "feature")} $failureString."
  }

  private def getPlural(i: Long, base: String): String = if (i == 1) s"$i $base" else s"$i ${base}s"
}