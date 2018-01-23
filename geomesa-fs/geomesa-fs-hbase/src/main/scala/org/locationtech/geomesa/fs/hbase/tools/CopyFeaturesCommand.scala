/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.hbase.tools

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, TimeUnit}

import com.beust.jcommander.{ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.hbase.tools.CopyFeaturesCommand.CopyFeaturesParams
import org.locationtech.geomesa.fs.hbase.tools.FsHBaseDataStoreCommand.FsHBaseParams
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.TextTools

import scala.util.control.NonFatal

class CopyFeaturesCommand extends FsHBaseDataStoreCommand with LazyLogging {

  override val name: String = "copy-features"
  override val params = new CopyFeaturesParams

  override def execute(): Unit = withDataStore(run)

  def run(hbaseDs: HBaseDataStore, fsDs: FileSystemDataStore): Unit = {
    val sft = fsDs.getSchema(params.featureName)
    if (sft == null) {
      throw new ParameterException(s"Schema '${params.featureName}' does not exist")
    }
    if (hbaseDs.getSchema(params.featureName) == null) {
      Command.user.info(s"Creating HBase schema ${params.featureName}...")
      hbaseDs.createSchema(sft)
      Command.user.info(s"Done")
    }

    val start = System.currentTimeMillis()
    val success = new AtomicLong(0L)
    val failed = new AtomicLong(0L)

    val callback = new Runnable {
      override def run(): Unit = {
        // use \r to replace current line
        // trailing space separates cursor
        System.err.print(s"\r$success written and $failed failed in ${TextTools.getTime(start)} ")
      }
    }

    Command.user.info(s"Copying features matching ${ECQL.toCQL(params.cqlFilter)}")
    val executor = Executors.newSingleThreadScheduledExecutor()
    executor.scheduleAtFixedRate(callback, 1, 1, TimeUnit.SECONDS)

    val query = new Query(params.featureName, params.cqlFilter)
    WithClose(
      fsDs.getFeatureReader(query, Transaction.AUTO_COMMIT),
      hbaseDs.getFeatureWriterAppend(params.featureName, Transaction.AUTO_COMMIT)) { case (reader, writer) =>
      while (reader.hasNext) {
        val feature = FeatureUtils.copyToWriter(writer, reader.next(), useProvidedFid = true)
        try {
          writer.write()
          success.incrementAndGet()
        } catch {
          case NonFatal(e) => logger.error(s"Error writing feature '$feature':", e); failed.incrementAndGet()
        }
      }
    }

    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.SECONDS)
    System.err.println

    Command.user.info(s"Bulk copy complete in ${TextTools.getTime(start)} with $success " +
        s"features copied and $failed features failed")
  }
}

object CopyFeaturesCommand {
  @Parameters(commandDescription = "Copy features from filesystem to Hbase")
  class CopyFeaturesParams extends FsHBaseParams with RequiredTypeNameParam with RequiredCqlFilterParam
}
