/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import com.beust.jcommander.validators.PositiveInteger
import com.beust.jcommander.{Parameter, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileStatus, Path, RemoteIterator}
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{Partition, StorageFile}
import org.locationtech.geomesa.fs.storage.common.metadata.FileBasedMetadataCatalog
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.fs.tools.ingest.FsManageMetadataCommand.CheckConsistencyCommand.ConsistencyChecker
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.locationtech.geomesa.tools.{Command, CommandWithSubCommands, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.concurrent.{CachedThreadPool, PhaserUtils}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.DateParsing

import java.io.{Closeable, FileNotFoundException}
import java.util.concurrent.{ConcurrentHashMap, Phaser}
import java.util.{Collections, Date}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

class FsManageMetadataCommand extends CommandWithSubCommands {

  import FsManageMetadataCommand.{CheckConsistencyCommand, ManageMetadataParams, RegisterCommand, UnregisterCommand}

  override val name: String = "manage-metadata"
  override val params = new ManageMetadataParams
  override val subCommands: Seq[Command] = Seq(new RegisterCommand(), new UnregisterCommand(), new CheckConsistencyCommand())
}

object FsManageMetadataCommand extends LazyLogging {

  import scala.collection.JavaConverters._

  private class RegisterCommand extends FsDataStoreCommand {

    override val name = "register"
    override val params = new RegisterParams()

    override def execute(): Unit = withDataStore { ds =>
      val storage = ds.storage(params.featureName)
      val metadata = storage.metadata

      val paths = params.files.asScala.map { file =>
        val path = new Path(file)
        if (!path.getFileSystem(storage.context.conf).exists(path)) {
          throw new IllegalArgumentException(s"File $path does not exist")
        }
        path
      }

      def outputResult(file: StorageFile): Unit = {
        Command.user.info(s"Registered file ${new Path(storage.context.root, file.file)} containing ${file.count} known features")
        val bounds =
          file.spatialBounds.map(b => s"${metadata.sft.getDescriptor(b.attribute).getLocalName} [${b.xmin},${b.ymin},${b.xmax},${b.ymax}]") ++
            file.attributeBounds.map { b =>
              val descriptor = metadata.sft.getDescriptor(b.attribute)
              val alias = AttributeIndexKey.alias(descriptor.getType.getBinding)
              val lower = AttributeIndexKey.decode(alias, b.lower) match {
                case null => ""
                case d: Date => DateParsing.formatDate(d)
                case v => v.toString
              }
              val upper = AttributeIndexKey.decode(alias, b.upper) match {
                case null => ""
                case d: Date => DateParsing.formatDate(d)
                case v => v.toString
              }
              s"${descriptor.getLocalName} [$lower,$upper]"
            }
        Command.user.info(s"File bounds:\n  ${bounds.mkString("\n  ")}")
      }

      try {
        paths.foreach { path =>
          outputResult(storage.register(path))
          if (params.delete) {
            path.getFileSystem(storage.context.conf).delete(path, false)
          }
        }
      } catch {
        case NonFatal(e) => throw new RuntimeException("Error registering file:", e)
      }


    }
  }

  private class UnregisterCommand extends FsDataStoreCommand {

    override val name = "unregister"
    override val params = new UnregisterParams

    override def execute(): Unit = withDataStore { ds =>
      val storage = ds.storage(params.featureName)
      val metadata = storage.metadata
      val file = StorageFile(params.file, Partition.None, 0L)
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
        val otherTypes = ds.getTypeNames.filter(_ != params.featureName).map(ds.storage).toSeq
        WithClose(new ConsistencyChecker(storage, otherTypes, params.threads))(_.run())
      }
    }
  }

  object CheckConsistencyCommand {

    class ConsistencyChecker(storage: FileSystemStorage, otherTypes: Seq[FileSystemStorage], threads: Int)
        extends Runnable with Closeable with LazyLogging {

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
          if (inconsistencies.nonEmpty) {
            lazy val strings = inconsistencies.map { i =>
              s"${i.file.file} (${if (i.duplicate) { "duplicate" } else { "missing" }})"
            }
            val msg = if (inconsistencies.size == 1) { "1 metadata entry that does" } else { s"${inconsistencies.size} metadata entries that do"}
            Command.user.warn(s"Found $msg not correspond to a data file:")
            Command.output.info(strings.sorted.mkString("  ", "\n  ", ""))
          }
          if (!onDisk.isEmpty) {
            // filter out files from other feature types in the same root
            otherTypes.foreach { s =>
              s.metadata.getFiles().foreach { f =>
                onDisk.remove(new Path(s.context.root, f.file))
              }
            }
            if (!onDisk.isEmpty) {
              val msg = if (onDisk.size() == 1) { "1 data file that does" } else { s"${onDisk.size} data files that do"}
              Command.user.warn(s"Found $msg not have a metadata entry:")
              if (otherTypes.nonEmpty) {
                Command.user.warn(s"Note: they may belong to one of the other registered feature types in this root")
              }
              Command.output.info(onDisk.asScala.map(_.toString).toSeq.sorted.mkString("  ", "\n  ", ""))
            }
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

  @Parameters(commandDescription = "Register new data files with a storage instance")
  private class RegisterParams extends FsParams with RequiredTypeNameParam {
    @Parameter(description = "Path of the file(s) to register", required = true)
    // noinspection VarCouldBeVal
    var files: java.util.List[String] = new java.util.ArrayList[String]()

    @Parameter(names = Array("--delete"), description = "Delete file(s) after registering them")
    // noinspection VarCouldBeVal
    var delete: java.lang.Boolean = false
  }

  @Parameters(commandDescription = "Unregister data files from a storage instance")
  private class UnregisterParams extends FsParams with RequiredTypeNameParam {
    @Parameter(names = Array("--file"), description = "Path of the file to unregister, relative to the storage root", required = true)
    // noinspection VarCouldBeVal
    var file: String = _
  }

  @Parameters(commandDescription = "Check consistency between metadata and data files")
  class CheckConsistencyParams extends FsParams with RequiredTypeNameParam {

    // TODO GEOMESA-2963 requires rebuilding file data stats
    // @Parameter(names = Array("--repair"), description = "Update metadata based on consistency check")
    // var repair: java.lang.Boolean = false

    @Parameter(
      names = Array("-t", "--threads"),
      description = "Number of concurrent threads to use",
      validateWith = Array(classOf[PositiveInteger]))
    var threads: Integer = 4
  }
}
