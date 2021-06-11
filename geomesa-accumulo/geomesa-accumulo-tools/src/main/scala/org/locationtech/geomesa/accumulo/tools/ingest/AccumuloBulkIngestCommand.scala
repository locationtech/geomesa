/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.ingest

import java.io.File

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.AccumuloDataStoreCommand.AccumuloDistributedCommand
import org.locationtech.geomesa.accumulo.tools.AccumuloDataStoreParams
import org.locationtech.geomesa.accumulo.tools.ingest.AccumuloBulkIngestCommand.AccumuloBulkIngestParams
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.jobs.JobResult.JobSuccess
import org.locationtech.geomesa.jobs.accumulo.mapreduce.GeoMesaAccumuloFileOutputFormat
import org.locationtech.geomesa.jobs.mapreduce.ConverterCombineInputFormat
import org.locationtech.geomesa.jobs.{Awaitable, JobResult, StatusCallback}
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes.RunMode
import org.locationtech.geomesa.tools.ingest.IngestCommand.{IngestParams, Inputs}
import org.locationtech.geomesa.tools.ingest._
import org.locationtech.geomesa.tools.utils.Prompt
import org.locationtech.geomesa.tools.{Command, OptionalCqlFilterParam, OptionalIndexParam, OutputPathParam}
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.io.fs.HadoopDelegate.HiddenFileFilter
import org.opengis.feature.simple.SimpleFeatureType

class AccumuloBulkIngestCommand extends IngestCommand[AccumuloDataStore] with AccumuloDistributedCommand {

  override val name = "bulk-ingest"
  override val params = new AccumuloBulkIngestParams()

  override protected def startIngest(
      mode: RunMode,
      ds: AccumuloDataStore,
      sft: SimpleFeatureType,
      converter: Config,
      inputs: Inputs): Awaitable = {

    val maxSplitSize =
      if (!params.combineInputs) { None } else { Option(params.maxSplitSize).map(_.intValue()).orElse(Some(0)) }

    // validate index param now that we have a datastore and the sft has been created
    val index = params.loadIndex(ds, sft.getTypeName, IndexMode.Write).map(_.identifier)

    val partitions = TablePartition(ds, sft).map { tp =>
      if (params.cqlFilter == null) {
        throw new ParameterException(
          s"Schema '${sft.getTypeName}' is a partitioned store. In order to bulk load, the '--cql' parameter " +
              "must be used to specify the range of the input data set")
      }
      tp.partitions(params.cqlFilter).filter(_.nonEmpty).getOrElse {
        throw new ParameterException(
          s"Partition filter does not correspond to partition scheme ${tp.getClass.getSimpleName}. Please specify " +
              "a valid filter using the '--cql' parameter")
      }
    }

    mode match {
      case RunModes.Local =>
        throw new IllegalArgumentException("Bulk ingest must be run in distributed mode")

      case RunModes.Distributed =>
        // file output format doesn't let you write to an existing directory
        val output = new Path(params.outputPath)
        val context = FileContext.getFileContext(output.toUri, new Configuration())
        if (context.util.exists(output)) {
          val warning = s"Output directory '$output' exists"
          if (params.force) {
            Command.user.warn(s"$warning - deleting it")
          } else if (!Prompt.confirm(s"WARNING DATA MAY BE LOST: $warning. Delete it and continue (y/n)? ")) {
            throw new ParameterException(s"Output directory '$output' exists")
          }
          context.delete(output, true)
        }

        Command.user.info(s"Running bulk ingestion in distributed ${if (params.combineInputs) "combine " else "" }mode")
        new BulkConverterIngest(ds, connection, sft, converter, inputs.paths, params.outputPath, maxSplitSize,
          index, partitions, libjarsFiles, libjarsPaths)

      case _ =>
        throw new NotImplementedError(s"Missing implementation for mode $mode")
    }
  }

  class BulkConverterIngest(
      ds: AccumuloDataStore,
      dsParams: Map[String, String],
      sft: SimpleFeatureType,
      converterConfig: Config,
      paths: Seq[String],
      output: String,
      maxSplitSize: Option[Int],
      index: Option[String],
      partitions: Option[Seq[String]],
      libjarsFiles: Seq[String],
      libjarsPaths: Iterator[() => Seq[File]]
    ) extends ConverterIngestJob(dsParams, sft, converterConfig, paths, libjarsFiles, libjarsPaths) {

    override def configureJob(job: Job): Unit = {
      super.configureJob(job)
      GeoMesaAccumuloFileOutputFormat.configure(job, ds, dsParams, sft, new Path(output), index, partitions)
      maxSplitSize.foreach { max =>
        job.setInputFormatClass(classOf[ConverterCombineInputFormat])
        if (max > 0) {
          FileInputFormat.setMaxInputSplitSize(job, max.toLong)
        }
      }
    }

    override def await(reporter: StatusCallback): JobResult = {
      super.await(reporter).merge {
        if (params.skipImport) {
          Command.user.info("Skipping import of RFiles into Accumulo")
          Some(JobSuccess(AccumuloBulkIngestCommand.ImportMessage, Map.empty))
        } else {
          Command.user.info("Importing RFiles into Accumulo")
          val tableOps = ds.connector.tableOperations()
          val filesPath = new Path(output, GeoMesaAccumuloFileOutputFormat.FilesPath)
          val fc = FileContext.getFileContext(filesPath.toUri, new Configuration())
          val files = fc.listLocatedStatus(filesPath)
          while (files.hasNext) {
            val file = files.next()
            val path = file.getPath
            val table = path.getName
            if (file.isDirectory && HiddenFileFilter.accept(path) && tableOps.exists(table)) {
              Command.user.info(s"Importing $table")
              tableOps.importDirectory(path.toString).to(table).load()
            }
          }
          Some(JobSuccess("", Map.empty))
        }
      }
    }
  }
}

object AccumuloBulkIngestCommand {

  private val ImportMessage =
    "\nFiles may be imported for each table through the Accumulo shell with the `importdirectory` command"

  @Parameters(commandDescription = "Convert various file formats into bulk loaded Accumulo RFiles")
  class AccumuloBulkIngestParams extends IngestParams with AccumuloDataStoreParams
      with OutputPathParam with OptionalIndexParam with OptionalCqlFilterParam {
    @Parameter(names = Array("--skip-import"), description = "Generate the files but skip the bulk import into Accumulo")
    var skipImport: Boolean = false
  }
}
