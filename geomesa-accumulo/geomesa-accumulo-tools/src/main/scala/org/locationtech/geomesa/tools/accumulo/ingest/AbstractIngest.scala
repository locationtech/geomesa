/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.ingest

import java.io._
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.geotools.data.{DataStoreFinder, DataUtilities, FeatureWriter, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.joda.time.Period
import org.joda.time.format.PeriodFormatterBuilder
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams
import org.locationtech.geomesa.utils.classpath.PathUtils
import org.locationtech.geomesa.utils.stats.CountingInputStream
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

/**
 * Base class for handling ingestion of local or distributed files
 *
 * @param dsParams data store parameters
 * @param typeName simple feature type name to ingest
 * @param inputs files to ingest
 * @param numLocalThreads for local ingest, how many threads to use
 */
abstract class AbstractIngest(val dsParams: Map[String, String],
                              typeName: String,
                              inputs: Seq[String],
                              numLocalThreads: Int) extends Runnable with LazyLogging {

  import org.locationtech.geomesa.tools.accumulo.ingest.AbstractIngest._

  /**
   * Setup hook - called before run method is executed
   */
  def beforeRunTasks(): Unit

  /**
   * Create a local ingestion converter
   *
   * @param file file being operated on
   * @param failures used to tracks failures
   * @return local converter
   */
  def createLocalConverter(file: File, failures: AtomicLong): LocalIngestConverter

  /**
   * Run a distributed ingestion
   *
   * @param statusCallback for reporting status
   * @return (success, failures) counts
   */
  def runDistributedJob(statusCallback: (Float, Long, Long, Boolean) => Unit): (Long, Long)

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

  /**
   * Main method to kick off ingestion
   */
  override def run(): Unit = {
    beforeRunTasks()
    val distPrefixes = Seq("hdfs://", "s3n://", "s3a://")
    if (distPrefixes.exists(inputs.head.toLowerCase.startsWith)) {
      logger.info("Running ingestion in distributed mode")
      runDistributed()
    } else {
      logger.info("Running ingestion in local mode")
      runLocal()
    }
    ds.dispose()
  }

  private def runLocal(): Unit = {

    // Global failure shared between threads
    val (written, failed) = (new AtomicLong(0), new AtomicLong(0))

    val bytesRead = new AtomicLong(0L)

    class LocalIngestWorker(file: File) extends Runnable {
      override def run(): Unit = {
        try {
          // only create the feature writer after the converter runs
          // so that we can create the schema based off the input file
          var fw: FeatureWriter[SimpleFeatureType, SimpleFeature] = null
          val converter = createLocalConverter(file, failed)
          // count the raw bytes read from the file, as that's what we based our total on
          val countingStream = new CountingInputStream(new FileInputStream(file))
          val is = PathUtils.handleCompression(countingStream, file.getPath)
          try {
            val (sft, features) = converter.convert(is)
            if (features.hasNext) {
              ds.createSchema(sft)
              fw = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)
            }
            features.foreach { sf =>
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
            IOUtils.closeQuietly(converter)
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
    val (success, failed) = runDistributedJob(status)
    logger.info(s"Distributed ingestion complete in ${getTime(start)}")
    logger.info(getStatInfo(success, failed))
  }
}

object AbstractIngest {

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
    val percent = f"${(progress * 100).toInt}%3d"
    val infoStr = s" $percent% complete $pass ingested $fail failed in ${getTime(start)}"

    // Figure out if and how much the progress bar should be scaled to accommodate smaller terminals
    val scaleFactor: Float = try {
      val tWidth: Float = jline.Terminal.getTerminal.getTerminalWidth.toFloat
      // Sanity check as jline may not be correct. We also don't scale up, ~112 := scaleFactor = 1.0f
      if (tWidth > infoStr.length + 3 && tWidth < emptyBar.length + infoStr.length + 2) {
        // Screen Width 80 yields scaleFactor of .46
        (tWidth - infoStr.length - 2) / emptyBar.length // -2 is for brackets around bar
      } else {
        1.0f
      }
    } catch {
      case _ => 1.0f
    }

    val scaledLen: Int = (emptyBar.length * scaleFactor).toInt
    val numDone = (scaledLen * progress).toInt
    val bar = if (numDone < 1) {
      emptyBar.substring(emptyBar.length - scaledLen)
    } else if (numDone >= scaledLen) {
      buildString(replacement, scaledLen)
    } else {
      val doneStr = buildString(replacement, numDone - 1) // -1 for indicator
      val doStr = emptyBar.substring(emptyBar.length - (scaledLen - numDone))
      s"$doneStr$indicator$doStr"
    }

    // use \r to replace current line
    // trailing space separates cursor
    out.print(s"\r[$bar]$infoStr")
    if (done) {
      out.println()
    }
  }

  private def buildString(c: Char, length: Int): String = {
    if (length < 0) return ""
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

trait LocalIngestConverter extends Closeable {
  def convert(is: InputStream): (SimpleFeatureType, Iterator[SimpleFeature])
}