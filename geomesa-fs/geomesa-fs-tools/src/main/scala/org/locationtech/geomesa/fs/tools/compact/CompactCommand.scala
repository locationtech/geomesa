/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.compact

import java.io.File

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.tools.ingest.TempDirParam
import org.locationtech.geomesa.fs.tools.{FsDataStoreCommand, FsParams, PartitionParam}
import org.locationtech.geomesa.tools.ingest.AbstractIngest
import org.locationtech.geomesa.tools.ingest.AbstractIngest.PrintProgress
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.locationtech.geomesa.utils.text.TextTools

import scala.collection.JavaConversions._

class CompactCommand extends FsDataStoreCommand with LazyLogging {

  override val name: String = "compact"
  override val params = new CompactParams

  override def execute(): Unit = {
    withDataStore(compact)
  }

  val libjarsFile: String = "org/locationtech/geomesa/fs/tools/ingest-libjars.list"

  def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_FS_HOME"),
    () => ClassPathUtils.getJarsFromClasspath(classOf[FileSystemDataStore])
  )

  def compact(ds: FileSystemDataStore): Unit = {
    Command.user.info(s"Beginning Compaction Process...updating metadata")

    ds.storage.updateMetadata(params.featureName)
    Command.user.info(s"Metadata update complete")

    val m = ds.storage.getMetadata(params.featureName)
    val allPartitions = m.getPartitions
    val toCompact: Seq[String] = if (params.partitions.nonEmpty) {
      params.partitions.filterNot(allPartitions.contains).headOption.foreach { p =>
        throw new ParameterException(s"Partition $p cannot be found in metadata")
      }
      params.partitions
    } else {
      allPartitions
    }
    Command.user.info(s"Compacting ${toCompact.size} partitions")

    params.runMode match {
      case "local" =>
        toCompact.foreach { p =>
          logger.info(s"Compacting ${params.featureName}:$p")
          ds.storage.compact(params.featureName, p)
          logger.info(s"Completed compaction of ${params.featureName}:$p")
        }

      case "distributed" =>
        withDataStore { ds =>
          val tempDir = Option(params.tempDir).map(t => new Path(t))
          val job = new ParquetCompactionJob(ds.getSchema(params.featureName), ds.root, tempDir)
          val statusCallback = new PrintProgress(System.err, TextTools.buildString(' ', 60), '\u003d', '\u003e', '\u003e')

          val start = System.currentTimeMillis()
          val (success, failed) = job.run(connection, params.featureName, toCompact, libjarsFile, libjarsPaths, statusCallback)
          Command.user.info(s"Distributed compaction complete in ${TextTools.getTime(start)}")
          Command.user.info(AbstractIngest.getStatInfo(success, failed))
        }
    }

    Command.user.info(s"Compaction completed")
  }
}

@Parameters(commandDescription = "Compact partitions")
class CompactParams extends FsParams with RequiredTypeNameParam with TempDirParam with PartitionParam {
  @Parameter(names = Array("--mode"), description = "Run mode for compaction ('local' or 'distributed' via mapreduce)", required = false)
  var runMode: String = "distributed"
}
