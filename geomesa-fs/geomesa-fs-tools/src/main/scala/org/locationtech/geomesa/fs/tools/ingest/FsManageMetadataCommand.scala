/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import com.beust.jcommander.converters.BaseConverter
import com.beust.jcommander.validators.PositiveInteger
import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileStatus, Path, RemoteIterator}
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{Partition, PartitionKey, SpatialBounds, StorageFile, StorageFileAction}
import org.locationtech.geomesa.fs.storage.common.metadata.FileBasedMetadataCatalog
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.fs.tools.ingest.FsManageMetadataCommand.CheckConsistencyCommand.ConsistencyChecker
import org.locationtech.geomesa.fs.tools.ingest.FsManageMetadataCommand._
import org.locationtech.geomesa.tools.{Command, CommandWithSubCommands, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.concurrent.{CachedThreadPool, PhaserUtils}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Envelope

import java.io.{Closeable, FileNotFoundException}
import java.util
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, Phaser}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

class FsManageMetadataCommand extends CommandWithSubCommands {
  override val name: String = "manage-metadata"
  override val params = new ManageMetadataParams
  override val subCommands: Seq[Command] = Seq(new RegisterCommand(), new UnregisterCommand(), new CheckConsistencyCommand())
}

object FsManageMetadataCommand {

  import scala.collection.JavaConverters._

  private class RegisterCommand extends FsDataStoreCommand {

    override val name = "register"
    override val params = new RegisterParams

    override def execute(): Unit = withDataStore { ds =>
      val storage = ds.storage(params.featureName)
      val metadata = storage.metadata
      val partition = Partition(params.partition.asScala.map(PartitionKey.apply).toSet)
      val count = Option(params.count).map(_.longValue()).getOrElse(0L)
      val env = new Envelope()
      Option(params.bounds).foreach { case (xmin, ymin, xmax, ymax) =>
        env.expandToInclude(xmin, ymin)
        env.expandToInclude(xmax, ymax)
      }
      val spatialBounds = SpatialBounds(metadata.sft.indexOf(metadata.sft.getGeometryDescriptor.getLocalName), env).toSeq
      // TODO other spatial bounds, attribute bounds
      val file = StorageFile(params.file, partition, count, StorageFileAction.Append, spatialBounds)
      val path = new Path(storage.context.root, file.file)
      if (!PathCache.exists(storage.context.fs, path)) {
        throw new IllegalArgumentException(s"File $path does not exist")
      }
      metadata.addFile(file)
      Command.user.info(s"Registered file $path containing $count known features")
    }
  }

  private class UnregisterCommand extends FsDataStoreCommand {

    override val name = "unregister"
    override val params = new UnregisterParams

    override def execute(): Unit = withDataStore { ds =>
      val storage = ds.storage(params.featureName)
      val metadata = storage.metadata
      val partition = Partition(params.partition.asScala.map(PartitionKey.apply).toSet)
      val file = StorageFile(params.file, partition, 0L)
      metadata.removeFile(file)
      Command.user.info(s"Unregistered file ${new Path(storage.context.root, file.file)}")
    }
  }

  class CheckConsistencyCommand extends FsDataStoreCommand with LazyLogging {

    override val name = "check-consistency"
    override val params = new CheckConsistencyParams()

    override def execute(): Unit = {
      withDataStore { ds =>
        Command.user.info("Checking consistency, please wait...")
        val storage = ds.storage(params.featureName)
        WithClose(new ConsistencyChecker(storage, params.threads))(_.run())
      }
    }
  }

  object CheckConsistencyCommand {

    class ConsistencyChecker(storage: FileSystemStorage, threads: Int) extends Runnable with Closeable with LazyLogging {

      private val pool = new CachedThreadPool(threads)
      private val onDisk = Collections.newSetFromMap(new ConcurrentHashMap[Path, java.lang.Boolean]())

      override def run(): Unit = {
        // list out the files currently in the root directory, results go into onDisk
        listRoot()

        val inconsistencies = ArrayBuffer.empty[Inconsistency]
        val checked = scala.collection.mutable.Set.empty[String]

        // compare the files known to the metadata to the ones on disk
        storage.metadata.getFiles().foreach { file =>
          if (onDisk.remove(new Path(storage.context.root, file.file))) {
            // metadata and file are consistent
            checked.add(file.file)
          } else {
            // file is missing from metadata vs file is in metadata more than once
            val duplicate = !checked.add(file.file)
            inconsistencies += Inconsistency(file, duplicate)
          }
        }

        if (onDisk.isEmpty && inconsistencies.isEmpty) {
          Command.user.info("No inconsistencies detected")
        } else {
          if (!onDisk.isEmpty) {
            Command.user.warn(s"Found ${onDisk.size} data files that do not have metadata entries:")
            Command.output.info(onDisk.asScala.map(_.toString).toSeq.sorted.mkString("  ", "\n  ", ""))
          }
          if (inconsistencies.nonEmpty) {
            lazy val strings = inconsistencies.map { i =>
              s"${i.file.file} (${if (i.duplicate) { "duplicate" } else { "missing" }})"
            }
            Command.user.warn(s"Found ${inconsistencies.size} metadata entries that do not correspond to a data file:")
            Command.output.info(strings.sorted.mkString("  ", "\n  ", ""))
          }
        }
      }

      override def close(): Unit = pool.shutdown()

      /**
       * List the files in the root directory. Excludes known file-based metadata files. Results are
       * added to onDisk
       */
      private def listRoot(): Unit = {
        val iter = storage.context.fs.listStatusIterator(storage.context.root)
        // use a phaser to track worker thread completion
        val phaser = new Phaser(2) // 1 for this thread + 1 for the worker
        pool.submit(new TopLevelListWorker(phaser, iter))
        // wait for the worker threads to complete
        phaser.awaitAdvanceInterruptibly(phaser.arrive())
      }

      private class TopLevelListWorker(phaser: Phaser, list: RemoteIterator[FileStatus]) extends Runnable {
        override def run(): Unit = {
          try {
            var i = phaser.getRegisteredParties + 1
            while (list.hasNext && i < PhaserUtils.MaxParties) {
              val status = list.next
              val path = status.getPath
              val name = path.getName
              if (status.isDirectory) {
                if (name != FileBasedMetadataCatalog.MetadataDirectory) {
                  i += 1
                  // use a tiered phaser on each directory avoid the limit of 65535 registered parties
                  pool.submit(new ListWorker(new Phaser(phaser, 1), name, storage.context.fs.listStatusIterator(path)))
                }
              } else {
                onDisk.add(path)
              }
            }
            if (list.hasNext) {
              pool.submit(new TopLevelListWorker(new Phaser(phaser, 1), list))
            }
          } finally {
            phaser.arriveAndDeregister()
          }
        }
      }

      private class ListWorker(phaser: Phaser, partition: String, listDirectory: => RemoteIterator[FileStatus])
          extends Runnable {
        override def run(): Unit = {
          try {
            var i = phaser.getRegisteredParties + 1
            val iter = listDirectory
            while (iter.hasNext && i < PhaserUtils.MaxParties) {
              val status = iter.next
              val path = status.getPath
              val name = path.getName
              if (status.isDirectory) {
                val nextPartition = s"$partition/$name"
                i += 1
                // use a tiered phaser on each directory avoid the limit of 65535 registered parties
                pool.submit(new ListWorker(new Phaser(phaser, 1), nextPartition, storage.context.fs.listStatusIterator(path)))
              } else {
                onDisk.add(path)
              }
            }
            if (iter.hasNext) {
              pool.submit(new ListWorker(new Phaser(phaser, 1), partition, iter))
            }
          } catch {
            case _: FileNotFoundException => // the partition dir was deleted... just return
            case NonFatal(e) => logger.error("Error scanning metadata directory:", e)
          } finally {
            phaser.arriveAndDeregister() // notify that this thread is done
          }
        }
      }
    }

    private case class Inconsistency(file: StorageFile, duplicate: Boolean)
  }

  @Parameters(commandDescription = "Manage the metadata for a storage instance")
  class ManageMetadataParams

  abstract class FilesParam extends FsParams with RequiredTypeNameParam {
    @Parameter(names = Array("--partition"), description = "Partition(s) that the file belongs to", required = true)
    var partition: java.util.List[String] = new util.ArrayList[String]()
  }

  @Parameters(commandDescription = "Register new data files with a storage instance")
  private class RegisterParams extends FilesParam {
    @Parameter(names = Array("--file"), description = "Path of the file to register, relative to the storage root", required = true)
    var file: String = _

    @Parameter(names = Array("--bounds"), description = "Geographic bounds of the data file being registered, in the form xmin,ymin,xmax,ymax", required = false, converter = classOf[BoundsConverter])
    var bounds: (Double, Double, Double, Double) = _

    @Parameter(names = Array("--count"), description = "Number of features in the data file being registered", required = false)
    var count: java.lang.Long = _
  }

  @Parameters(commandDescription = "Unregister data files from a storage instance")
  private class UnregisterParams extends FilesParam {
    @Parameter(names = Array("--file"), description = "Path of the file to unregister, relative to the storage root", required = true)
    var file: String = _
  }

  @Parameters(commandDescription = "Check consistency between metadata and data files")
  class CheckConsistencyParams extends FsParams with RequiredTypeNameParam {

    // TODO GEOMESA-2963 requires rebuilding file data stats
    // @Parameter(names = Array("--repair"), description = "Update metadata based on consistency check")
    // var repair: java.lang.Boolean = false
    //
    // @Parameter(names = Array("--rebuild"), description = "Replace all current metadata from the data files")
    // var rebuild: java.lang.Boolean = false

    @Parameter(
      names = Array("-t", "--threads"),
      description = "Number of concurrent threads to use",
      validateWith = Array(classOf[PositiveInteger]))
    var threads: Integer = 4
  }

  private class BoundsConverter(name: String) extends BaseConverter[(Double, Double, Double, Double)](name) {
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
