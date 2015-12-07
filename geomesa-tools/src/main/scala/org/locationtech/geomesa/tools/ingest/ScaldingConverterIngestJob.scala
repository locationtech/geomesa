/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.ingest

import java.net.URLDecoder
import java.nio.charset.StandardCharsets

import com.twitter.scalding.{Args, Hdfs, Job, Local, Mode}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.{DataStoreFinder, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory.{params => dsp}
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.convert.Transformers.DefaultCounter
import org.locationtech.geomesa.jobs.scalding.MultipleUsefulTextLineFiles
import org.locationtech.geomesa.tools.Utils.IngestParams

class ScaldingConverterIngestJob(args: Args) extends Job(args) with Logging {
  import scala.collection.JavaConversions._

  val counter = new DefaultCounter

  lazy val pathList         = DelimitedIngest.decodeFileList(args(IngestParams.FILE_PATH))
  lazy val featureName      = args(IngestParams.FEATURE_NAME)
  lazy val converterConfig  = URLDecoder.decode(args(IngestParams.CONVERTER_CONFIG), StandardCharsets.UTF_8.displayName)
  lazy val isTestRun        = args(IngestParams.IS_TEST_INGEST).toBoolean

  //Data Store parameters
  lazy val dsConfig =
    Map(
      dsp.zookeepersParam.getName -> args(IngestParams.ZOOKEEPERS),
      dsp.instanceIdParam.getName -> args(IngestParams.ACCUMULO_INSTANCE),
      dsp.tableNameParam.getName  -> args(IngestParams.CATALOG_TABLE),
      dsp.userParam.getName       -> args(IngestParams.ACCUMULO_USER),
      dsp.passwordParam.getName   -> args(IngestParams.ACCUMULO_PASSWORD),
      dsp.authsParam.getName      -> args.optional(IngestParams.AUTHORIZATIONS),
      dsp.visibilityParam.getName -> args.optional(IngestParams.VISIBILITIES),
      dsp.mockParam.getName       -> args.optional(IngestParams.ACCUMULO_MOCK)
    ).collect{ case (key, Some(value)) => (key, value); case (key, value: String) => (key, value) }

  // non-serializable resources.
  class Resources {
    val ds = DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore]
    val sft = ds.getSchema(featureName)
    lazy val fw = ds.getFeatureWriterAppend(featureName, Transaction.AUTO_COMMIT)
    val converter = SimpleFeatureConverters.build[String](sft, ConfigFactory.parseString(converterConfig))
    val callback = converter.processWithCallback(counter = counter)
    def release(): Unit = {
      logger.trace("Releasing ingest resources")
      converter.close()
      fw.close()
    }
  }

  def printStatInfo() {
    Mode.getMode(args) match {
      case Some(Local(_)) =>
        logger.info(getStatInfo(counter.getSuccess, counter.getFailure, "Local ingest completed, total lines:"))
      case Some(Hdfs(_, _)) =>
        logger.info("Ingest completed in HDFS mode")
      case _ =>
        logger.warn("Could not determine job mode")
    }
  }

  def getStatInfo(successes: Int, failures: Int, pref: String): String = {
    val successPvsS = if (successes == 1) "feature" else "features"
    val failurePvsS = if (failures == 1) "feature" else "features"
    val failureString = if (failures == 0) "with no failures" else s"and failed to ingest: $failures $failurePvsS"
    s"$pref ${counter.getLineCount}, ingested: $successes $successPvsS, $failureString."
  }

  // Check to see if this an actual ingest job or just a test.
  if (!isTestRun) {
    new MultipleUsefulTextLineFiles(pathList: _*).using(new Resources)
      .foreach('line) { (cres: Resources, s: String) =>
        processLine(cres, s)
      }
  }

  private def processLine(resources: Resources, s: String) =
    resources.callback(s)
      .foreach { sf =>
        val toWrite = resources.fw.next()
        toWrite.setAttributes(sf.getAttributes)
        toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
        toWrite.getUserData.putAll(sf.getUserData)
        toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        resources.fw.write()
      }

  def runTestIngest(lines: Iterator[String]) = {
    val ds = DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore]
    ds.createSchema(ds.getSchema(featureName))
    val res = new Resources
    try {
      lines.foreach(processLine(res, _))
    } finally {
      res.release()
    }
  }

}
