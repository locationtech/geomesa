/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.ingest

import java.io.InputStream
import java.net.URLDecoder
import java.nio.charset.StandardCharsets

import com.twitter.scalding.{Args, Hdfs, Job, Local, Mode}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.commons.io.IOUtils
import org.geotools.data.{DataStoreFinder, Transaction}
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory.{params => dsp}
import org.locationtech.geomesa.accumulo.index.Constants
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, DefaultCounter}
import org.locationtech.geomesa.jobs.scalding.UsefulFileSource
import org.locationtech.geomesa.tools.Utils.IngestParams
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

class ScaldingConverterIngestJob(args: Args) extends Job(args) with Logging {
  import scala.collection.JavaConversions._

  val counter = new DefaultCounter

  lazy val pathList         = DelimitedIngest.decodeFileList(args(IngestParams.FILE_PATH))
  lazy val sftSpec          = URLDecoder.decode(args(IngestParams.SFT_SPEC), "UTF-8")
  lazy val featureName      = args(IngestParams.FEATURE_NAME)
  lazy val converterConfig  = args(IngestParams.CONVERTER_CONFIG)
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

  lazy val sft = {
    val ret = SimpleFeatureTypes.createType(featureName, sftSpec)
    args.optional(IngestParams.INDEX_SCHEMA_FMT).foreach { indexSchema =>
      ret.getUserData.put(Constants.SFT_INDEX_SCHEMA, indexSchema)
    }
    ret
  }

  // non-serializable resources.
  class Resources {
    val ds = DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore]
    val fw = ds.getFeatureWriterAppend(featureName, Transaction.AUTO_COMMIT)
    val converter = SimpleFeatureConverters.build[String](sft, ConfigFactory.parseString(converterConfig))
    def release(): Unit = { fw.close() }
  }

  def printStatInfo() {
    Mode.getMode(args) match {
      case Some(Local(_)) =>
        logger.info(getStatInfo(counter.getSuccess(), counter.getFailure(), "Local ingest completed, total features:"))
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
    s"$pref ${counter.getCount()}, ingested: $successes $successPvsS, $failureString."
  }

  // Check to see if this an actual ingest job or just a test.
  if (!isTestRun) {
    new UsefulFileSource(pathList: _*).using(new Resources)
      .foreach('file) { (cres: Resources, is: InputStream) =>
        processInputStream(cres, is)
      }
  }

  private def processInputStream(resources: Resources, inputStream: InputStream) = {
    val fw = resources.fw
    val converter = SimpleFeatureConverters.build[String](sft, ConfigFactory.parseString(converterConfig))
    try {
      converter
        .processInput(IOUtils.lineIterator(inputStream, StandardCharsets.UTF_8), counter = counter)
        .foreach { sf =>
          val toWrite = fw.next()
          toWrite.setAttributes(sf.getAttributes)
          toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
          toWrite.getUserData.putAll(sf.getUserData)
          fw.write()
        }
    } finally {
      fw.close()
    }
  }

  def runTestIngest(lines: Iterator[String]) = {
    val ds = DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore]
    ds.createSchema(sft)
    val fw = ds.getFeatureWriterAppend(featureName, Transaction.AUTO_COMMIT)
    val converter = SimpleFeatureConverters.build[String](sft, ConfigFactory.parseString(converterConfig))
    try {
      converter
        .processInput(lines, counter = counter)
        .foreach { sf =>
          val toWrite = fw.next()
          toWrite.setAttributes(sf.getAttributes)
          toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
          toWrite.getUserData.putAll(sf.getUserData)
          fw.write()
        }
    } finally {
      fw.close()
    }
  }

}


