/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, DataUtilities, FeatureWriter, Transaction}
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.EvaluationContext.DelegatingEvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.jobs.JobResult.{JobFailure, JobSuccess}
import org.locationtech.geomesa.jobs._
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestCounters
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
import org.locationtech.geomesa.utils.io.fs.LocalDelegate.StdInHandle
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, PathUtils, WithClose}
import org.locationtech.geomesa.utils.text.TextTools
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.Flushable
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors}
import scala.util.control.NonFatal

/**
 * Ingestion that uses geomesa converters to process input files
 *
 * @param ds data store
 * @param sft simple feature type
 * @param converterConfig converter definition
 * @param inputs paths to ingest
 * @param numThreads how many threads to use
 */
class LocalConverterIngest(
    ds: DataStore,
    sft: SimpleFeatureType,
    converterConfig: Config,
    inputs: Seq[String],
    numThreads: Int
  ) extends Awaitable with LazyLogging {

  // if inputs is empty, we've already validated that stdin has data to read
  private val stdin = inputs.isEmpty
  private val files = if (stdin) { StdInHandle.available().toSeq } else { inputs.flatMap(PathUtils.interpretPath) }
  private val latch = new CountDownLatch(files.length)

  private val threads = if (numThreads <= files.length) { numThreads } else {
    Command.user.warn("Can't use more threads than there are input files - reducing thread count")
    files.length
  }

  private val batch = IngestCommand.LocalBatchSize.toInt.getOrElse {
    throw new IllegalArgumentException(
      s"Invalid batch size for property ${IngestCommand.LocalBatchSize.property}: " +
          IngestCommand.LocalBatchSize.get)
  }

  private val es = Executors.newFixedThreadPool(threads)

  // global counts shared among threads
  private val written = new AtomicLong(0)
  private val failed = new AtomicLong(0)
  private val errors = new AtomicInteger(0)

  private val bytesRead = new AtomicLong(0L)

  private val batches = new ConcurrentHashMap[FeatureWriter[SimpleFeatureType, SimpleFeature], AtomicInteger](threads)

  // keep track of failure at a global level, keep line counts and success local
  private val globalFailures = new com.codahale.metrics.Counter {
    override def inc(): Unit = failed.incrementAndGet()
    override def inc(n: Long): Unit = failed.addAndGet(n)
    override def dec(): Unit = failed.decrementAndGet()
    override def dec(n: Long): Unit = failed.addAndGet(-1 * n)
    override def getCount: Long = failed.get()
  }

  private val totalLength: () => Float = if (stdin) {
    () => (bytesRead.get + files.map(_.length).sum).toFloat // re-evaluate each time as bytes are read from stdin
  } else {
    val length = files.map(_.length).sum.toFloat // only evaluate once
    () => length
  }

  Command.user.info(s"Ingesting ${if (stdin) { "from stdin" } else { TextTools.getPlural(files.length, "file") }} " +
      s"with ${TextTools.getPlural(threads, "thread")}")

  private val converters = CloseablePool(SimpleFeatureConverter(sft, converterConfig), threads)
  private val writers = CloseablePool(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT), threads)

  private val closer = new Runnable() {
    override def run(): Unit = {
      latch.await()
      CloseWithLogging(converters)
      CloseWithLogging(writers).foreach(_ => errors.incrementAndGet())
    }
  }

  private val futures =
    files.map(f => es.submit(new LocalIngestWorker(f))).toList :+ CachedThreadPool.submit(closer)

  es.shutdown()

  override def await(reporter: StatusCallback): JobResult = {
    while (!es.isTerminated) {
      Thread.sleep(500)
      reporter("", progress(), counters, done = false)
    }
    reporter("", progress(), counters, done = true)

    // Get all futures so that we can propagate the logging up to the top level for handling
    // in org.locationtech.geomesa.tools.Runner to catch missing dependencies
    futures.foreach(_.get)

    if (errors.get > 0) {
      JobFailure("Some files caused errors, check logs for details")
    } else {
      val message =
        if (stdin) { "from stdin" } else if (files.lengthCompare(1) == 0) { s"for file ${files.head.path}" } else { "" }
      JobSuccess(message, counters.toMap)
    }
  }

  /**
   * Hook to allow modification of the feature returned by the converter
   *
   * @param iter features
   * @return
   */
  protected def features(iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = iter

  private def progress(): Float = bytesRead.get() / totalLength()
  private def counters: Seq[(String, Long)] =
    Seq((IngestCounters.Ingested, written.get()), (IngestCounters.Failed, failed.get()))

  class LocalIngestWorker(file: FileHandle) extends Runnable {
    override def run(): Unit = {
      try {
        converters.borrow { converter =>
          val delegate = converter.createEvaluationContext(EvaluationContext.inputFileParam(file.path))
          val ec = new DelegatingEvaluationContext(delegate)(failure = globalFailures)
          WithClose(file.open) { streams =>
            streams.foreach { case (name, is) =>
              ec.setInputFilePath(name.getOrElse(file.path))
              WithClose(LocalConverterIngest.this.features(converter.process(is, ec))) { features =>
                writers.borrow { writer =>
                  var count = batches.get(writer)
                  if (count == null) {
                    count = new AtomicInteger(0)
                    batches.put(writer, count)
                  }
                  features.foreach { sf =>
                    try {
                      FeatureUtils.write(writer, sf, useProvidedFid = true)
                      written.incrementAndGet()
                      count.incrementAndGet()
                    } catch {
                      case NonFatal(e) =>
                        logger.error(s"Failed to write '${DataUtilities.encodeFeature(sf)}'", e)
                        failed.incrementAndGet()
                    }
                    if (count.get % batch == 0) {
                      count.set(0)
                      writer match {
                        case f: Flushable => f.flush()
                        case _ => // no-op
                      }
                    }
                  }
                }
              }
            }
          }
        }
      } catch {
        case e @ (_: ClassNotFoundException | _: NoClassDefFoundError) =>
          // Rethrow exception so it can be caught by getting the future of this runnable in the main thread
          // which will in turn cause the exception to be handled by org.locationtech.geomesa.tools.Runner
          // Likely all threads will fail if a dependency is missing so it will terminate quickly
          throw e

        case NonFatal(e) =>
          // Don't kill the entire program b/c this thread was bad! use outer try/catch
          val msg = s"Fatal error running local ingest worker on ${file.path}"
          Command.user.error(msg)
          logger.error(msg, e)
          errors.incrementAndGet()
      } finally {
        latch.countDown()
        bytesRead.addAndGet(file.length)
      }
    }
  }
}
