/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data._
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.data._
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.index.utils.FeatureWriterHelper
import org.locationtech.geomesa.jobs.JobResult.{JobFailure, JobSuccess}
import org.locationtech.geomesa.jobs._
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.IngestCommand.{IngestCounters, Inputs}
import org.locationtech.geomesa.tools.ingest.LocalConverterIngest.DataStoreWriter
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
import org.locationtech.geomesa.utils.text.TextTools

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
    dsParams: java.util.Map[String, _],
    sft: SimpleFeatureType,
    converterConfig: Config,
    inputs: Inputs,
    numThreads: Int
  ) extends Awaitable with LazyLogging {

  private val files = inputs.handles
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

  // keep track of failure at a global level, but we don't count successes until they're written to the store
  private val listener = new EvaluationContext.ContextListener {
    override def onSuccess(i: Int): Unit = {}
    override def onFailure(i: Int): Unit = failed.addAndGet(i)
  }

  private val progress: () => Float =
    if (inputs.stdin) {
      () => .99f // we don't know how many bytes are actually available
    } else {
      val length = files.map(_.length).sum.toFloat // only evaluate once
      () => bytesRead.get / length
    }

  Command.user.info(s"Ingesting ${if (inputs.stdin) { "from stdin" } else { TextTools.getPlural(files.length, "file") }} " +
      s"with ${TextTools.getPlural(threads, "thread")}")

  private val converters = CloseablePool(SimpleFeatureConverter(sft, converterConfig), threads)
  private val writers = {
    def factory: FeatureWriter[SimpleFeatureType, SimpleFeature] =
      if (threads > 1 && ds.getClass.getSimpleName.equals("JDBCDataStore")) {
        // creates a new data store for each writer to avoid synchronized blocks in JDBCDataStore.
        // the synchronization is to allow for generated fids from the database.
        // generally, this shouldn't be an issue since we use provided fids,
        // but running with 1 thread  would restore the old behavior
        new DataStoreWriter(dsParams, sft.getTypeName)
      } else {
        ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
      }
    CloseablePool(factory, threads)
  }

  private val closer = new Runnable() {
    override def run(): Unit = {
      latch.await()
      CloseWithLogging(converters)
      CloseWithLogging(writers).foreach(_ => errors.incrementAndGet())
    }
  }

  private val futures = files.map(f => es.submit(new LocalIngestWorker(f))) :+ CachedThreadPool.submit(closer)

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
        if (inputs.stdin) { "from stdin" } else if (files.lengthCompare(1) == 0) { s"for file ${files.head.path}" } else { "" }
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

  private def counters: Seq[(String, Long)] =
    Seq((IngestCounters.Ingested, written.get()), (IngestCounters.Failed, failed.get()))

  class LocalIngestWorker(file: FileHandle) extends Runnable {
    override def run(): Unit = {
      try {
        converters.borrow { converter =>
          WithClose(file.open) { streams =>
            streams.foreach { case (name, is) =>
              val params = EvaluationContext.inputFileParam(name.getOrElse(file.path))
              val ec = converter.createEvaluationContext(params).withListener(listener)
              WithClose(LocalConverterIngest.this.features(converter.process(is, ec))) { features =>
                writers.borrow { writer =>
                  var count = batches.get(writer)
                  if (count == null) {
                    count = new AtomicInteger(0)
                    batches.put(writer, count)
                  }
                  val helper = FeatureWriterHelper(writer)
                  features.foreach { sf =>
                    try {
                      helper.write(sf)
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

object LocalConverterIngest {

  class DataStoreWriter(connection: java.util.Map[String, _], typeName: String)
      extends FeatureWriter[SimpleFeatureType, SimpleFeature] {

    private val ds = DataStoreFinder.getDataStore(connection)
    private val writer = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)

    override def getFeatureType: SimpleFeatureType = writer.getFeatureType
    override def next(): SimpleFeature = writer.next()
    override def remove(): Unit = writer.remove()
    override def write(): Unit = writer.write()
    override def hasNext: Boolean = writer.hasNext
    override def close(): Unit = {
      var err: Throwable = null
      CloseQuietly(writer).foreach(err = _)
      CloseQuietly(ds).foreach { e =>
        if (err == null) { err = e } else { err.addSuppressed(e) }
      }
      if (err != null) {
        throw err
      }
    }
  }
}
