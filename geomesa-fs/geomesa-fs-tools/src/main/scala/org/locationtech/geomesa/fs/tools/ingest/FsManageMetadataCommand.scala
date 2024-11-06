/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import com.beust.jcommander.converters.BaseConverter
import com.beust.jcommander.validators.PositiveInteger
import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileStatus, RemoteIterator}
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionBounds, PartitionMetadata, StorageFile}
import org.locationtech.geomesa.fs.storage.common.metadata.{FileBasedMetadataFactory, MetadataJson}
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsParams, PartitionParam}
import org.locationtech.geomesa.fs.tools.ingest.FsManageMetadataCommand.CheckConsistencyCommand.ConsistencyChecker
import org.locationtech.geomesa.fs.tools.ingest.FsManageMetadataCommand._
import org.locationtech.geomesa.tools.utils.Prompt
import org.locationtech.geomesa.tools.{Command, CommandWithSubCommands, OptionalForceParam, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.concurrent.{CachedThreadPool, PhaserUtils}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Envelope

import java.io.{Closeable, FileNotFoundException}
import java.util
import java.util.concurrent.{ConcurrentHashMap, Phaser}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

class FsManageMetadataCommand extends CommandWithSubCommands {
  override val name: String = "manage-metadata"
  override val params = new ManageMetadataParams
  override val subCommands: Seq[Command] =
    Seq(new CompactCommand(), new RegisterCommand(), new UnregisterCommand(), new CheckConsistencyCommand())
}

object FsManageMetadataCommand {

  import scala.collection.JavaConverters._

  class CompactCommand extends FsDataStoreCommand {

    override val name = "compact"
    override val params = new CompactParams

    override def execute(): Unit = withDataStore { ds =>
      Command.user.info("Compacting metadata, please wait...")
      val metadata = ds.storage(params.featureName).metadata
      metadata.compact(None, None, params.threads)
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
      val files = params.files.asScala.map(StorageFile(_, System.currentTimeMillis()))
      val count = Option(params.count).map(_.longValue()).getOrElse(0L)
      val bounds = new Envelope
      Option(params.bounds).foreach { case (xmin, ymin, xmax, ymax) =>
        bounds.expandToInclude(xmin, ymin)
        bounds.expandToInclude(xmax, ymax)
      }
      metadata.addPartition(PartitionMetadata(params.partition, files.toSeq, PartitionBounds(bounds), count))
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
      val files = params.files.asScala.map(StorageFile(_, 0L))
      val count = Option(params.count).map(_.longValue()).getOrElse(0L)
      metadata.removePartition(PartitionMetadata(params.partition, files.toSeq, None, count))
      val partition = metadata.getPartition(params.partition).getOrElse(PartitionMetadata("", Seq.empty, None, 0L))
      Command.user.info(s"Unregistered ${params.files.size} files. Updated partition: ${partition.files.size} " +
          s"files containing ${partition.count} known features")
    }
  }

  class CheckConsistencyCommand extends FsDataStoreCommand with LazyLogging {

    override val name = "check-consistency"
    override val params = new CheckConsistencyParams()

    override def validate(): Option[ParameterException] = {
      if (params.repair && params.rebuild) {
        Some(new ParameterException("Please specify at most one of --repair and --rebuild"))
      } else {
        None
      }
    }

    override def execute(): Unit = {
      if (params.rebuild && !params.partitions.isEmpty) {
        Command.user.warn(
          s"Rebuilding all metadata using only ${params.partitions.size} partitions - " +
              "metadata will not exist for any other partitions")
        if (!params.force && !Prompt.confirm("Continue (y/n)? ")) {
          Command.user.info("Cancelling operation")
          return
        }
      }

      withDataStore { ds =>
        Command.user.info("Checking consistency, please wait...")
        val storage = ds.storage(params.featureName)
        val partitions = Option(params.partitions).collect { case p if !p.isEmpty => p.asScala.toSeq }
        WithClose(new ConsistencyChecker(storage, partitions, params.rebuild, params.repair, params.threads))(_.run())
      }
    }
  }

  object CheckConsistencyCommand {

    class ConsistencyChecker(
        storage: FileSystemStorage,
        partitions: Option[Seq[String]],
        rebuild: Boolean,
        repair: Boolean,
        threads: Int
      ) extends Runnable with Closeable with LazyLogging {

      private val pool = new CachedThreadPool(threads)
      private val onDisk = new ConcurrentHashMap[String, ConcurrentHashMap[String, Boolean]]()

      private val computeFunction = new java.util.function.Function[String, ConcurrentHashMap[String, Boolean]] {
        override def apply(t: String): ConcurrentHashMap[String, Boolean] = new ConcurrentHashMap[String, Boolean]()
      }

      override def run(): Unit = {
        storage.metadata.invalidate() // invalidate any cached state

        // list out the files currently in the root directory, results go into onDisk
        listRoot()

        if (rebuild) {
          val (count, partitions) = buildPartitionConfigs()
          Command.user.info(s"Setting metadata to ${partitions.length} partitions containing $count data files...")
          storage.metadata.setPartitions(partitions)
          Command.user.info("Done")
        } else {
          val inconsistencies = scala.collection.mutable.Map.empty[String, ArrayBuffer[Inconsistency]]
          var inconsistentCount = 0

          // compare the files known to the metadata to the ones on disk
          storage.metadata.getPartitions().foreach { partition =>
            if (partitions.forall(_.contains(partition.name))) {
              val checked = scala.collection.mutable.Set.empty[String]
              val partitionOnDisk = onDisk.getOrDefault(partition.name, new ConcurrentHashMap[String, Boolean]())
              partition.files.foreach { file =>
                if (partitionOnDisk.remove(file.name)) {
                  // metadata and file are consistent
                  checked.add(file.name)
                  // TODO if (params.analyze) {} // update metadata
                } else {
                  val buf = inconsistencies.getOrElseUpdate(partition.name, ArrayBuffer.empty)
                  // file is missing from metadata vs file in in metadata more than once
                  val duplicate = !checked.add(file.name)
                  buf += Inconsistency(file, duplicate)
                  inconsistentCount += 1
                }
              }
              if (partitionOnDisk.isEmpty) {
                onDisk.remove(partition.name)
              }
            }
          }

          if (onDisk.isEmpty && inconsistencies.isEmpty) {
            Command.user.info("No inconsistencies detected")
          } else if (repair) {
            if (inconsistencies.nonEmpty) {
              Command.user.info(s"Removing $inconsistentCount inconsistent metadata references...")
              inconsistencies.foreach { case (partition, files) =>
                storage.metadata.removePartition(PartitionMetadata(partition, files.map(_.file).toSeq, None, 0L))
              }
              Command.user.info("Done")
            }
            if (!onDisk.isEmpty) {
              val (count, partitions) = buildPartitionConfigs()
              Command.user.info(s"Registering $count missing metadata references...")
              partitions.foreach(storage.metadata.addPartition)
              Command.user.info("Done")
            }
            // TODO if (params.analyze) {} // update metadata
          } else {
            if (!onDisk.isEmpty) {
              lazy val strings = onDisk.asScala.toSeq.flatMap { case (partition, files) =>
                files.asScala.map { case (f, _) => s"$partition,$f" }
              }
              Command.user.warn(s"Found ${strings.length} data files that do not have metadata entries:")
              Command.output.info(strings.sorted.mkString("  ", "\n  ", ""))
            }
            if (inconsistencies.nonEmpty) {
              lazy val strings = inconsistencies.toSeq.flatMap { case (p, buf) =>
                buf.map { case Inconsistency(f, d) => s"$p,${if (d) { "duplicate" } else { "missing" }},${f.name}" }
              }
              Command.user.warn(s"Found $inconsistentCount metadata entries that do not correspond to a data file:")
              Command.output.info(strings.sorted.mkString("  ", "\n  ", ""))
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

      private def buildPartitionConfigs(): (Int, Seq[PartitionMetadata]) = {
        var count = 0
        val partitions = Seq.newBuilder[PartitionMetadata]
        onDisk.asScala.foreach { case (partition, files) =>
          val update = Seq.newBuilder[StorageFile]
          files.asScala.foreach { case (name, _) =>
            update += StorageFile(name, System.currentTimeMillis())
            count += 1
          }
          partitions += PartitionMetadata(partition, update.result, None, 0L)
        }
        (count, partitions.result)
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
                if (name != FileBasedMetadataFactory.MetadataDirectory &&
                    partitions.forall(_.exists(p => p == name || p.startsWith(s"$name/")))) {
                  i += 1
                  // use a tiered phaser on each directory avoid the limit of 65535 registered parties
                  pool.submit(new ListWorker(new Phaser(phaser, 1), name, storage.context.fs.listStatusIterator(path)))
                }
              } else if (name != MetadataJson.MetadataPath) {
                onDisk.computeIfAbsent("", computeFunction).put(name, java.lang.Boolean.TRUE)
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
                if (partitions.forall(_.exists(p => p == nextPartition || p.startsWith(s"$nextPartition/")))) {
                  i += 1
                  // use a tiered phaser on each directory avoid the limit of 65535 registered parties
                  pool.submit(new ListWorker(new Phaser(phaser, 1), nextPartition, storage.context.fs.listStatusIterator(path)))
                }
              } else {
                val leafPartition =
                  if (storage.metadata.leafStorage) { s"$partition/${StorageUtils.leaf(name)}" } else { partition }
                if (partitions.forall(_.contains(leafPartition))) {
                  onDisk.computeIfAbsent(leafPartition, computeFunction).put(name, java.lang.Boolean.TRUE)
                }
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

  @Parameters(commandDescription = "Check consistency between metadata and data files")
  class CheckConsistencyParams
      extends FsParams with RequiredTypeNameParam with PartitionParam with OptionalForceParam {

    @Parameter(names = Array("--repair"), description = "Update metadata based on consistency check")
    var repair: java.lang.Boolean = false

    @Parameter(names = Array("--rebuild"), description = "Replace all current metadata from the data files")
    var rebuild: java.lang.Boolean = false

    // TODO GEOMESA-2963
    // @Parameter(names = Array("--analyze"), description = "Rebuild file data statistics (may be slow)")
    // var analyze: java.lang.Boolean = false

    @Parameter(
      names = Array("-t", "--threads"),
      description = "Number of concurrent threads to use",
      validateWith = Array(classOf[PositiveInteger]))
    var threads: Integer = 4
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
