/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.tools.ingest

import com.beust.jcommander.Parameters
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.geotools.data.DataStore
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.jobs.HBaseIndexFileMapper
import org.locationtech.geomesa.hbase.tools.ingest.HBaseBulkIngestCommand.HBaseBulkIngestParams
import org.locationtech.geomesa.hbase.tools.ingest.HBaseIngestCommand.HBaseIngestParams
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools.ingest.AbstractConverterIngest.StatusCallback
import org.locationtech.geomesa.tools.ingest.DistributedCombineConverterIngest.ConverterCombineIngestJob
import org.locationtech.geomesa.tools.ingest.DistributedConverterIngest.ConverterIngestJob
import org.locationtech.geomesa.tools.ingest._
import org.locationtech.geomesa.tools.{Command, OutputPathParam, RequiredIndexParam}
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.feature.simple.SimpleFeatureType


class HBaseBulkIngestCommand extends HBaseIngestCommand {

  override val name = "bulk-ingest"
  override val params = new HBaseBulkIngestParams()

  override protected def createIngest(mode: RunMode,
                                      sft: SimpleFeatureType,
                                      converter: Config,
                                      inputs: Seq[String]): Runnable = {
    mode match {
      case RunModes.Local =>
        throw new IllegalArgumentException("Bulk ingest must be run in distributed mode")

      case RunModes.Distributed =>
        new DistributedConverterIngest(connection, sft, converter, inputs, libjarsFile, libjarsPaths,
          params.waitForCompletion) with BulkConverterIngest {

          override protected def createJob(): ConverterIngestJob =
            new ConverterIngestJob(connection, sft, converter, inputs, libjarsFile, libjarsPaths) {
              override def configureJob(job: Job): Unit = {
                super.configureJob(job)
                HBaseIndexFileMapper.configure(job, connection, sft.getTypeName, index, new Path(params.outputPath))
              }
            }
        }

      case RunModes.DistributedCombine =>
        new DistributedCombineConverterIngest(connection, sft, converter, inputs, libjarsFile, libjarsPaths,
          Option(params.maxSplitSize), params.waitForCompletion) with BulkConverterIngest {

          override protected def createJob(): ConverterCombineIngestJob =
            new ConverterCombineIngestJob(connection, sft, converter, inputs, Option(params.maxSplitSize), libjarsFile, libjarsPaths) {
              override def configureJob(job: Job): Unit = {
                super.configureJob(job)
                HBaseIndexFileMapper.configure(job, connection, sft.getTypeName, index, new Path(params.outputPath))
              }
            }
        }

      case _ =>
        throw new NotImplementedError(s"Missing implementation for mode $mode")
    }
  }

  trait BulkConverterIngest extends AbstractConverterIngest {

    // validate index param now that we have a datastore and the sft has been created
    var index: String = _

    abstract override protected def runIngest(ds: DataStore, sft: SimpleFeatureType, callback: StatusCallback): Unit = {
      index = params.loadIndex(ds.asInstanceOf[HBaseDataStore], sft.getTypeName, IndexMode.Write).identifier
      super.runIngest(ds, sft, callback)
      Command.user.info("To load files, run:\n\tgeomesa-hbase bulk-load " +
          s"-c ${params.catalog} -f ${sft.getTypeName} --index ${params.index} --input ${params.outputPath}")
    }
  }
}

object HBaseBulkIngestCommand {
  @Parameters(commandDescription = "Convert various file formats into HBase HFiles suitable for incremental load")
  class HBaseBulkIngestParams extends HBaseIngestParams with RequiredIndexParam with OutputPathParam
}
