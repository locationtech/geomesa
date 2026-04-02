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
import org.apache.hadoop.fs.{FileStatus, FileUtil, Path, RemoteIterator}
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{AttributeBounds, Partition, PartitionKey, SpatialBounds, StorageFile, StorageFileAction}
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.{FileTracker, UpdateObserver}
import org.locationtech.geomesa.fs.storage.common.metadata.FileBasedMetadataCatalog
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType
import org.locationtech.geomesa.fs.storage.common.utils.{PathCache, StorageUtils}
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.fs.tools.ingest.FsManageMetadataCommand.CheckConsistencyCommand.ConsistencyChecker
import org.locationtech.geomesa.fs.tools.ingest.FsManageMetadataCommand._
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.locationtech.geomesa.tools.{Command, CommandWithSubCommands, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.concurrent.{CachedThreadPool, PhaserUtils}
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.io.WithClose

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
      if (params.copy) {
        val from = new Path(params.file)
        if (!from.getFileSystem(storage.context.conf).exists(from)) {
          throw new IllegalArgumentException(s"File $from does not exist")
        }
        val relativePath = StorageUtils.nextFile(metadata.sft.getTypeName, FileType.Written, storage.encoding)
        val destination = new Path(storage.context.root, relativePath)
        Command.user.info(s"Copying input file $from to $destination")
        FileUtil.copy(from.getFileSystem(storage.context.conf), from, storage.context.fs, destination, false, storage.context.conf)
        params.file = relativePath
        PathCache.register(storage.context.fs, destination)
      }

      val path = new Path(storage.context.root, params.file)
      if (!PathCache.exists(storage.context.fs, path)) {
        throw new IllegalArgumentException(s"File $path does not exist")
      }

      val sort = params.sort.asScala.map { name =>
        val i = metadata.sft.indexOf(name)
        if (i == -1) {
          throw new ParameterException(s"Invalid attribute for sort order, does not exist in the feature type: $name")
        }
        i
      }

      val file = if (params.calculateMetadata) {
        val reader = storage match {
          case s: AbstractFileSystemStorage => s.createReader(None, None)
          case s => throw new UnsupportedOperationException(s"--calculate-metadata is not supported for storage class ${s.getClass.getName}")
        }
        val tracker = new FileTracker(metadata.sft, Set.empty)
        val partitions = new java.util.HashSet[Partition]()
        WithClose(new UpdateObserver(tracker, Partition.None, params.file, StorageFileAction.Append)) { observer =>
          WithClose(reader.read(params.file)) { iter =>
            if (!iter.hasNext) {
              throw new RuntimeException("Could not read any features from input file")
            }
            iter.foreach { sf =>
              partitions.add(Partition(metadata.schemes.map(_.getPartition(sf))))
              observer(sf)
            }
          }
        }
        if (partitions.size() != 1) {
            throw new IllegalArgumentException(
              s"File corresponds to multiple partitions: ${partitions.asScala.mkString(" AND ")}")
        }
        tracker.getFiles().head.copy(partition = partitions.iterator().next())
      } else {
        val partition = Partition(params.partition.asScala.map(PartitionKey.apply).toSet)
        if (partition.values.isEmpty) {
          throw new ParameterException("Must either --calculate-metadata or specify --partition for the file")
        }
        val count = Option(params.count).map(_.longValue()).getOrElse(0L)
        val spatialBounds = params.spatialBounds.asScala.map { case (name, bounds) =>
          val i = metadata.sft.indexOf(name)
          if (i == -1) {
            throw new ParameterException(s"Invalid attribute for spatial bounds, does not exist in the feature type: $name")
          }
          bounds.copy(attribute = i)
        }
        val attributeBounds = params.attributeBounds.asScala.map { case (name, bounds) =>
          val i = metadata.sft.indexOf(name)
          if (i == -1) {
            throw new ParameterException(s"Invalid attribute for attribute bounds, does not exist in the feature type: $name")
          }
          val binding = metadata.sft.getDescriptor(i).getType.getBinding
          val lower = AttributeIndexKey.TypeRegistry.encode(FastConverter.convert(bounds.lower, binding))
          val upper = AttributeIndexKey.TypeRegistry.encode(FastConverter.convert(bounds.upper, binding))
          AttributeBounds(i, lower, upper)
        }
        StorageFile(params.file, partition, count, StorageFileAction.Append, spatialBounds.toSeq, attributeBounds.toSeq, sort.toSeq)
      }

      metadata.addFile(file)
      Command.user.info(s"Registered file $path containing ${file.count} known features")
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
  // noinspection VarCouldBeVal
  private class RegisterParams extends FsParams with RequiredTypeNameParam {
    @Parameter(names = Array("--file"), description = "Path of the file to register, relative to the storage root", required = true)
    var file: String = _

    @Parameter(
      names = Array("--copy-file"),
      description = "Copy the file into the storage root. If copying, the --file path should be an absolute path instead of relative to the storage root",
      required = false)
    var copy: java.lang.Boolean = false

    @Parameter(
      names = Array("--calculate-metadata"),
      description = "Read the data file being registered in order to store metadata for querying",
      required = false)
    var calculateMetadata: java.lang.Boolean = false

    @Parameter(names = Array("--partition"), description = "Partition(s) that the file belongs to", required = false)
    var partition: java.util.List[String] = new util.ArrayList[String]()

    @Parameter(
      names = Array("--spatial-bounds"),
      description = "Geographic bounds for a geometry of the data file being registered, in the form <attribute>=xmin,ymin,xmax,ymax",
      required = false,
      converter = classOf[SpatialBoundsConverter])
    var spatialBounds: java.util.List[(String, SpatialBounds)] = new java.util.ArrayList[(String, SpatialBounds)]()

    @Parameter(
      names = Array("--attribute-bounds"),
      description = "Lower and upper bounds for an attribute of the data file being registered, in the form <attribute>=lower,upper",
      required = false,
      converter = classOf[AttributeBoundsConverter])
    var attributeBounds: java.util.List[(String, AttributeBounds)] = new java.util.ArrayList[(String, AttributeBounds)]()

    @Parameter(names = Array("--count"), description = "Number of features in the data file being registered", required = false)
    var count: java.lang.Long = _

    @Parameter(names = Array("--sort"), description = "Name of any attributes that the file is sorted by", required = false)
    var sort: java.util.List[String] = new java.util.ArrayList[String]()
  }

  @Parameters(commandDescription = "Unregister data files from a storage instance")
  private class UnregisterParams extends FsParams with RequiredTypeNameParam {
    @Parameter(names = Array("--file"), description = "Path of the file to unregister, relative to the storage root", required = true)
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

  private class SpatialBoundsConverter(name: String) extends BaseConverter[(String, SpatialBounds)](name) {
    override def convert(value: String): (String, SpatialBounds) = {
      try {
        val Array(name, bounds) = value.split("=", 2).map(_.trim)
        val Array(xmin, ymin, xmax, ymax) = bounds.split(",").map(_.trim.toDouble)
        (name, SpatialBounds(-1, xmin, ymin, xmax, ymax))
      } catch {
        case NonFatal(e) => throw new ParameterException(getErrorString(value, s"format: $e"))
      }
    }
  }
  private class AttributeBoundsConverter(name: String) extends BaseConverter[(String, AttributeBounds)](name) {
    override def convert(value: String): (String, AttributeBounds) = {
      try {
        val Array(name, bounds) = value.split("=", 2).map(_.trim)
        val Array(lower, upper) = bounds.split("/").map(_.trim)
        (name, AttributeBounds(-1, lower, upper))
      } catch {
        case NonFatal(e) => throw new ParameterException(getErrorString(value, s"format: $e"))
      }
    }
  }
}
