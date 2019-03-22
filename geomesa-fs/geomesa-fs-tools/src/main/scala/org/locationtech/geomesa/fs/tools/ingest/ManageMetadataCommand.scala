/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.util

import com.beust.jcommander.converters.BaseConverter
import com.beust.jcommander.{JCommander, Parameter, ParameterException, Parameters}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionBounds, PartitionMetadata}
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.fs.tools.ingest.ManageMetadataCommand.{CompactCommand, ManageMetadataParams, RegisterCommand, UnregisterCommand}
import org.locationtech.geomesa.tools.{Command, CommandWithSubCommands, RequiredTypeNameParam, Runner}
import org.locationtech.jts.geom.Envelope

import scala.util.control.NonFatal

class ManageMetadataCommand(val runner: Runner, val jc: JCommander) extends CommandWithSubCommands {

  override val name: String = "manage-metadata"
  override val params = new ManageMetadataParams

  override val subCommands: Seq[Command] = Seq(new CompactCommand, new RegisterCommand, new UnregisterCommand)
}

object ManageMetadataCommand {

  import scala.collection.JavaConverters._

  class CompactCommand extends FsDataStoreCommand {

    override val name = "compact"
    override val params = new CompactParams

    override def execute(): Unit = withDataStore { ds =>
      val metadata = ds.storage(params.featureName).metadata
      metadata.compact(None, params.threads)
      val partitions = metadata.getPartitions()
      Command.user.info(s"Compacted metadata into ${partitions.length} partitions consisting of " +
          s"${partitions.map(_.files.size).sum} files")
    }
  }

  class RegisterCommand extends FsDataStoreCommand {

    override val name = "register"
    override val params = new RegisterParams

    override def execute(): Unit = withDataStore { ds =>
      val metadata = ds.storage(params.featureName).metadata
      val count = Option(params.count).map(_.longValue()).getOrElse(0L)
      val bounds = new Envelope
      Option(params.bounds).foreach { case (xmin, ymin, xmax, ymax) =>
        bounds.expandToInclude(xmin, ymin)
        bounds.expandToInclude(xmax, ymax)
      }
      metadata.addPartition(PartitionMetadata(params.partition, params.files.asScala, PartitionBounds(bounds), count))
      val partition = metadata.getPartition(params.partition).getOrElse(PartitionMetadata("", Seq.empty, None, 0L))
      Command.user.info(s"Registered ${params.files.size} new files. Updated partition: ${partition.files.size} " +
          s"files containing ${partition.count} known features")
    }
  }

  class UnregisterCommand extends FsDataStoreCommand {

    override val name = "unregister"
    override val params = new UnregisterParams

    override def execute(): Unit = withDataStore { ds =>
      val metadata = ds.storage(params.featureName).metadata
      val count = Option(params.count).map(_.longValue()).getOrElse(0L)
      metadata.removePartition(PartitionMetadata(params.partition, params.files.asScala, None, count))
      val partition = metadata.getPartition(params.partition).getOrElse(PartitionMetadata("", Seq.empty, None, 0L))
      Command.user.info(s"Unregistered ${params.files.size} files. Updated partition: ${partition.files.size} " +
          s"files containing ${partition.count} known features")
    }
  }

  @Parameters(commandDescription = "Manage the metadata for a storage instance")
  class ManageMetadataParams

  @Parameters(commandDescription = "Compact the metadata for a storage instance")
  class CompactParams extends FsParams with RequiredTypeNameParam {
    @Parameter(names = Array("-t", "--threads"), description = "Number of threads to use for compaction")
    var threads: Integer = 4
  }

  @Parameters(commandDescription = "Register new data files with a storage instance")
  class RegisterParams extends FsParams with RequiredTypeNameParam {
    @Parameter(names = Array("--partition"), description = "Partition to update", required = true)
    var partition: String = _

    @Parameter(names = Array("--files"), description = "Names of the files to register, must already exist in the appropriate partition folder", required = true, variableArity = true)
    var files: java.util.List[String] = new util.ArrayList[String]()

    @Parameter(names = Array("--bounds"), description = "Geographic bounds of the data files being registered, in the form xmin,ymin,xmax,ymax", required = false, converter = classOf[BoundsConverter])
    var bounds: (Double, Double, Double, Double) = _

    @Parameter(names = Array("--count"), description = "Number of features in the data files being registered", required = false)
    var count: java.lang.Long = _
  }

  @Parameters(commandDescription = "Unregister data files from a storage instance")
  class UnregisterParams extends FsParams with RequiredTypeNameParam {
    @Parameter(names = Array("--partition"), description = "Partition to update", required = true)
    var partition: String = _

    @Parameter(names = Array("--files"), description = "Names of the files to unregister, must already exist in the appropriate partition folder", required = true, variableArity = true)
    var files: java.util.List[String] = new util.ArrayList[String]()

    @Parameter(names = Array("--count"), description = "Number of features in the data files being unregistered", required = false)
    var count: java.lang.Long = _
  }

  class BoundsConverter(name: String) extends BaseConverter[(Double, Double, Double, Double)](name) {
    override def convert(value: String): (Double, Double, Double, Double) = {
      try {
        val Array(xmin, ymin, xmax, ymax) = value.split(",").map(_.trim.toDouble)
        (xmin, ymin, xmax, ymax)
      } catch {
        case NonFatal(e) => throw new ParameterException(getErrorString(value, s"format: $e"))
      }
    }
  }
}
