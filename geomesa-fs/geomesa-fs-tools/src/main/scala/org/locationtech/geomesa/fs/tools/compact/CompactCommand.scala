/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.compact

import java.io.File

import com.beust.jcommander.{ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemStorage
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{PartitionParam, FsParams}
import org.locationtech.geomesa.fs.tools.compact.CompactCommand.CompactParams
import org.locationtech.geomesa.fs.tools.compact.FileSystemCompactionJob.{OrcCompactionJob, ParquetCompactionJob}
import org.locationtech.geomesa.fs.tools.ingest.FsIngestCommand.TempDirParam
import org.locationtech.geomesa.parquet.ParquetFileSystemStorage
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools.ingest.AbstractIngest
import org.locationtech.geomesa.tools.ingest.AbstractIngest.PrintProgress
import org.locationtech.geomesa.tools.{Command, DistributedRunParam, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.locationtech.geomesa.utils.io.PathUtils
import org.locationtech.geomesa.utils.text.TextTools

import scala.collection.JavaConversions._

class CompactCommand extends FsDataStoreCommand with LazyLogging {

  private val libjarsFileName = "org/locationtech/geomesa/fs/tools/ingest-libjars.list"

  private def libjarsSearchPath: Iterator[() => Seq[File]] = Iterator(
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_FS_HOME"),
    () => ClassPathUtils.getJarsFromClasspath(classOf[FileSystemDataStore])
  )

  override val name: String = "compact"
  override val params = new CompactParams

  override def execute(): Unit = withDataStore(compact)

  def compact(ds: FileSystemDataStore): Unit = {
    Command.user.info(s"Beginning Compaction Process...")
    Command.user.info(s"Updating metadata to ensure consistency")

    val storage = ds.storage(params.featureName)

    storage.updateMetadata()
    Command.user.info(s"Metadata update complete")

    val allPartitions = storage.getPartitions
    val toCompact: Seq[String] = if (params.partitions.nonEmpty) {
      params.partitions.filterNot(allPartitions.contains).headOption.foreach { p =>
        throw new ParameterException(s"Partition $p cannot be found in metadata")
      }
      params.partitions
    } else {
      allPartitions
    }
    Command.user.info(s"Compacting ${toCompact.size} partitions")

    val mode = Option(params.mode).getOrElse {
      if (PathUtils.isRemote(storage.getMetadata.getRoot.toString)) {
        RunModes.Distributed
      } else {
        RunModes.Local
      }
    }

    mode match {
      case RunModes.Local =>
        toCompact.foreach { p =>
          logger.info(s"Compacting $p")
          storage.compact(p)
        }

      case RunModes.Distributed =>
        val encoding = storage.getMetadata.getEncoding
        val job = if (encoding == ParquetFileSystemStorage.ParquetEncoding) {
          new ParquetCompactionJob()
        } else if (encoding == OrcFileSystemStorage.OrcEncoding) {
          new OrcCompactionJob()
        } else {
          throw new ParameterException(s"Compaction is not supported for encoding '$encoding'")
        }
        val tempDir = Option(params.tempDir).map(t => new Path(t))
        val statusCallback = new PrintProgress(System.err, TextTools.buildString(' ', 60), '\u003d', '\u003e', '\u003e')

        val start = System.currentTimeMillis()
        val (success, failed) = job.run(connection, params.featureName, toCompact, tempDir,
          libjarsFileName, libjarsSearchPath, statusCallback)
        Command.user.info(s"Distributed compaction complete in ${TextTools.getTime(start)}")
        Command.user.info(AbstractIngest.getStatInfo(success, failed, "Compacted"))
    }

    Command.user.info(s"Compaction completed")
  }
}

object CompactCommand {
  @Parameters(commandDescription = "Compact partitions")
  class CompactParams extends FsParams
      with RequiredTypeNameParam with TempDirParam with PartitionParam with DistributedRunParam
}
