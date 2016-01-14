/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.ingest

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStoreFinder, DataUtilities, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.convert.Transformers.DefaultCounter
import org.locationtech.geomesa.utils.classpath.PathUtils
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.util.Try

class DelimitedIngest(dsParams: Map[String, String], sft: SimpleFeatureType, converterConfig: Config, inputs: Seq[String])
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
    val counter = new DefaultCounter
    val fw = ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
    val converter = SimpleFeatureConverters.build[String](sft, converterConfig)
    val xml = Try(converterConfig.getString("converter.type") == "xml").getOrElse(false)
    val files = inputs.flatMap(PathUtils.interpretPath)
    files.foreach { f =>
      val ec = converter.createEvaluationContext(Map("inputFilePath" -> f.getAbsolutePath), counter)
      val source = PathUtils.getSource(f) // handles gzip
      try {
        val converted = if (xml) {
          converter.processInput(Iterator(source.mkString), ec) // process as a single line
        } else {
          converter.processInput(source.getLines(), ec)
        }
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
        source.close()
      }
    }
    fw.close()
    logger.info(s"Local ingestion complete: ${getStatInfo(counter.getSuccess, counter.getFailure)}")
  }

  private def runDistributed(): Unit = {
    val (success, failed) = ConverterIngestJob.run(dsParams, sft, converterConfig, inputs)
    logger.info(s"Distributed ingestion complete: ${getStatInfo(success, failed)}")
  }

  def getStatInfo(successes: Long, failures: Long): String = {
    val successPvsS = if (successes == 1) "feature" else "features"
    val failurePvsS = if (failures == 1) "feature" else "features"
    val failureString = if (failures == 0) "with no failures" else s"and failed to ingest: $failures $failurePvsS"
    s"ingested: $successes $successPvsS $failureString."
  }
}
