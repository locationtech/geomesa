/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.ingest

import java.io.File
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, TimeUnit}

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.geotools.data.{DataStoreFinder, DataUtilities, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.utils.classpath.PathUtils
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class ConverterIngest(dsParams: Map[String, String],
                      sft: SimpleFeatureType,
                      converterConfig: Config,
                      inputs: Seq[String],
                      numLocalThreads: Int)
    extends Runnable with LazyLogging {

  val ds = DataStoreFinder.getDataStore(dsParams)

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
    class ThreadSafeCounter extends org.locationtech.geomesa.convert.Transformers.Counter {
      private val (s, f, c) = (new AtomicLong(0), new AtomicLong(0), new AtomicLong(0))

      override def incSuccess(i: Long): Unit   = s.getAndAdd(i)
      override def incFailure(i: Long): Unit   = f.getAndAdd(i)
      override def incLineCount(i: Long): Unit = c.getAndAdd(i)
      override def setLineCount(i: Long): Unit = c.set(i)
      override def getFailure: Long            = f.get()
      override def getLineCount: Long          = c.get()
      override def getSuccess: Long            = s.get()
    }
    val counter = new ThreadSafeCounter

    class LocalIngestWorker(file: File) extends Runnable {

      override def run(): Unit = {}
        val fw        = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
        val converter = SimpleFeatureConverters.build(sft, converterConfig)
        val ec        = converter.createEvaluationContext(Map("inputFilePath" -> file.getAbsolutePath), counter)
        val is        = PathUtils.getInputStream(file)
        try {
          val converted = converter.process(is, ec)
          converted.foreach { sf =>
            val toWrite = fw.next()
            toWrite.setAttributes(sf.getAttributes)
            toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
            toWrite.getUserData.putAll(sf.getUserData)
            toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            try {
              fw.write()
            } catch {
              case e: Exception => logger.error(s"Failed to write '${DataUtilities.encodeFeature(toWrite)}'", e)
            }
          }
        } finally {
          IOUtils.closeQuietly(is)
          IOUtils.closeQuietly(fw)
        }
    }

    val files = inputs.flatMap(PathUtils.interpretPath)

    logger.info(s"Ingesting with $numLocalThreads thread${if (numLocalThreads > 1) "s" else "" }")
    val es = Executors.newFixedThreadPool(1)
    files.foreach(f => es.submit(new LocalIngestWorker(f)))
    es.shutdown()
    es.awaitTermination(4, TimeUnit.DAYS)

    logger.info(s"Local ingestion complete: ${getStatInfo(counter.getSuccess, counter.getFailure)}")
  }


  private def runDistributed(): Unit = {
    val (success, failed) = ConverterIngestJob.run(dsParams, sft, converterConfig, inputs)
    logger.info(s"Distributed ingestion complete: ${getStatInfo(success, failed)}")
  }

  def getStatInfo(successes: Long, failures: Long): String = {
    val successPvsS   = if (successes == 1) "feature" else "features"
    val failurePvsS   = if (failures == 1) "feature" else "features"
    val failureString = if (failures == 0) "with no failures" else s"and failed to ingest: $failures $failurePvsS"
    s"ingested: $successes $successPvsS $failureString."
  }
}
