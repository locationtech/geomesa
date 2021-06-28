/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.compact

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.storage.orc.OrcFileSystemStorage
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorage
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsDistributedCommand, FsParams, PartitionParam}
import org.locationtech.geomesa.fs.tools.compact.FileSystemCompactionJob.{OrcCompactionJob, ParquetCompactionJob}
import org.locationtech.geomesa.fs.tools.compact.FsCompactCommand.CompactCommand
import org.locationtech.geomesa.jobs.JobResult.{JobFailure, JobSuccess}
import org.locationtech.geomesa.tools.Command.CommandException
import org.locationtech.geomesa.tools.DistributedRunParam.RunModes
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.tools.utils.ParameterConverters.BytesConverter
import org.locationtech.geomesa.tools.utils.TerminalCallback.PrintProgress
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
import org.locationtech.geomesa.tools._
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
import org.locationtech.geomesa.tools._
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
import org.locationtech.geomesa.tools._
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
import org.locationtech.geomesa.tools._
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
=======
import org.locationtech.geomesa.tools._
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
import org.locationtech.geomesa.tools._
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4a51affd9e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
import org.locationtech.geomesa.utils.io.PathUtils
import org.locationtech.geomesa.utils.text.TextTools

import java.util.Locale
import java.util.concurrent.{CountDownLatch, Executors}
import scala.util.control.NonFatal

// need to mixin FsDistributedCommand to pick up base libjars file
class FsCompactCommand extends CompactCommand with FsDistributedCommand

object FsCompactCommand {

  trait CompactCommand extends FsDataStoreCommand with DistributedCommand with LazyLogging {

    import scala.collection.JavaConverters._

    override val name: String = "compact"
    override val params = new CompactParams

    override def libjarsFiles: Seq[String] = Seq("org/locationtech/geomesa/tools/ingest-libjars.list")

    override def execute(): Unit = withDataStore(compact)

    def compact(ds: FileSystemDataStore): Unit = {
      Command.user.info("Beginning compaction process...")

      val storage = ds.storage(params.featureName)

      val toCompact = if (params.partitions.isEmpty) { storage.getPartitions } else {
        val filtered = params.partitions.asScala.flatMap(storage.metadata.getPartition)
        if (filtered.lengthCompare(params.partitions.size()) != 0) {
          val unmatched = params.partitions.asScala.filterNot(name => filtered.exists(_.name == name))
          throw new ParameterException(s"Partition(s) ${unmatched.mkString(", ")} cannot be found in metadata")
        }
        filtered
      }

      val mode = params.mode.getOrElse {
        if (PathUtils.isRemote(storage.context.root.toString)) { RunModes.Distributed } else { RunModes.Local }
      }
      val fileSize = Option(params.targetFileSize).map(_.longValue)

      Command.user.info(s"Compacting ${toCompact.size} partitions in ${mode.toString.toLowerCase(Locale.US)} mode")

      val start = System.currentTimeMillis()
      val status = new PrintProgress(System.err, TextTools.buildString(' ', 60), '\u003d', '\u003e', '\u003e')

      mode match {
        case RunModes.Local =>
          val total = toCompact.length
          val latch = new CountDownLatch(total)
          val executor = Executors.newFixedThreadPool(math.max(1, math.min(params.threads, total)))

          try {
            toCompact.foreach { p =>
              executor.submit(
                new Runnable() {
                  override def run(): Unit = {
                    try {
                      logger.info(s"Compacting ${p.name}")
                      storage.compact(Some(p.name), fileSize)
                    } catch {
                      case NonFatal(e) => logger.error(s"Error processing partition '${p.name}':", e)
                    } finally {
                      latch.countDown()
                    }
                  }
                }
              )
            }
          } finally {
            executor.shutdown()
          }

          while (latch.getCount > 0) {
            Thread.sleep(1000)
            status("", 1f - latch.getCount.toFloat / total, Seq.empty, done = false)
          }
          status("", 1f, Seq.empty, done = true)
          Command.user.info("Compacting metadata")
          storage.metadata.compact(None, None, math.max(1, params.threads))
          Command.user.info(s"Local compaction complete in ${TextTools.getTime(start)}")

        case RunModes.Distributed =>
          val encoding = storage.metadata.encoding
          val job = if (ParquetFileSystemStorage.Encoding.equalsIgnoreCase(encoding)) {
            new ParquetCompactionJob()
          } else if (OrcFileSystemStorage.Encoding.equalsIgnoreCase(encoding)) {
            new OrcCompactionJob()
          } else {
            throw new ParameterException(s"Compaction is not supported for encoding '$encoding'")
          }
          val tempDir = Option(params.tempPath).map(t => new Path(t))
<<<<<<< HEAD
          job.run(storage, toCompact.toSeq, fileSize, tempDir, libjarsFiles, libjarsPaths, status) match {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
          job.run(storage, toCompact, fileSize, tempDir, libjarsFiles, libjarsPaths, status) match {
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
          job.run(storage, toCompact, fileSize, tempDir, libjarsFiles, libjarsPaths, status) match {
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
          job.run(storage, toCompact, fileSize, tempDir, libjarsFiles, libjarsPaths, status) match {
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
          job.run(storage, toCompact, fileSize, tempDir, libjarsFiles, libjarsPaths, status) match {
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
=======
          job.run(storage, toCompact, fileSize, tempDir, libjarsFiles, libjarsPaths, status) match {
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
          job.run(storage, toCompact, fileSize, tempDir, libjarsFiles, libjarsPaths, status) match {
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4a51affd9e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
            case JobSuccess(message, counts) =>
              Command.user.info(s"Distributed compaction complete in ${TextTools.getTime(start)}")
              val success = counts(FileSystemCompactionJob.MappedCounter)
              val failed = counts(FileSystemCompactionJob.FailedCounter)
              Command.user.info(IngestCommand.getStatInfo(success, failed, "Compacted", message))

            case JobFailure(message) =>
              Command.user.error(s"Distributed compaction failed in ${TextTools.getTime(start)}")
              throw new CommandException(message)
          }
      }
    }
  }

  @Parameters(commandDescription = "Compact partitions")
  class CompactParams extends FsParams
      with RequiredTypeNameParam with TempPathParam with PartitionParam with DistributedRunParam {

    @Parameter(names = Array("-t", "--threads"), description = "Number of threads if using local mode")
    var threads: Integer = 4

    @Parameter(
      names = Array("--target-file-size"),
      description = "Target size for data files",
      converter = classOf[BytesConverter])
    var targetFileSize: java.lang.Long = _
  }
}
