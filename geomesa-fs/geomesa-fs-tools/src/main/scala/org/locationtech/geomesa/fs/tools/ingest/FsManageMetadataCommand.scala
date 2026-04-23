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
import org.geotools.api.data.DataStoreFinder
import org.locationtech.geomesa.fs.data.{FileSystemDataStore, FileSystemDataStoreParams}
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.StorageFile
import org.locationtech.geomesa.fs.storage.core.metadata.FileBasedMetadataCatalog
import org.locationtech.geomesa.fs.storage.core.{FileSystemStorage, Partition, StorageKeys}
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsParams, MetadataTypeValidator}
import org.locationtech.geomesa.fs.tools.ingest.FsManageMetadataCommand.CheckConsistencyCommand.ConsistencyChecker
import org.locationtech.geomesa.fs.tools.ingest.FsManageMetadataCommand.MigrateCommand
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.locationtech.geomesa.tools.utils.NoopParameterSplitter
import org.locationtech.geomesa.tools.utils.ParameterConverters.KeyValueConverter
import org.locationtech.geomesa.tools.{Command, CommandWithSubCommands, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.{CachedThreadPool, PhaserUtils}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.DateParsing

import java.io._
import java.net.URI
import java.util.concurrent.{ConcurrentHashMap, Phaser}
import java.util.{Collections, Date, Properties}
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

class FsManageMetadataCommand extends CommandWithSubCommands {

  import FsManageMetadataCommand.{CheckConsistencyCommand, ManageMetadataParams, RegisterCommand, UnregisterCommand}

  override val name: String = "manage-metadata"
  override val params = new ManageMetadataParams
  override val subCommands: Seq[Command] =
    Seq(new RegisterCommand(), new UnregisterCommand(), new CheckConsistencyCommand(), new MigrateCommand())
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
        val path = {
          val tmp = new URI(file)
          if (tmp.getScheme == null || tmp.getScheme.isEmpty) {
            new URI(storage.context.root.getScheme, tmp.getHost, tmp.getPath, tmp.getFragment)
          } else if (tmp.getScheme == storage.context.root.getScheme) {
            tmp
          } else {
            throw new IllegalArgumentException(
              s"File $file must have the same scheme as the storage context: ${storage.context.root}")
          }
        }
        if (!storage.fs.exists(path)) {
          throw new IllegalArgumentException(s"File $path does not exist")
        }
        path
      }

      def outputResult(file: StorageFile): Unit = {
        Command.user.info(s"Registered file ${storage.context.root.resolve(file.file)} containing ${file.count} known features")
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
            storage.fs.delete(path)
          }
        }
      } catch {
        case NonFatal(e) => throw new RuntimeException("Error registering file:", e)
      }
    }
  }

  private class UnregisterCommand extends FsDataStoreCommand {

    override val name = "unregister"
    override val params = new UnregisterParams()

    override def execute(): Unit = withDataStore { ds =>
      val storage = ds.storage(params.featureName)
      val metadata = storage.metadata
      val file = StorageFile(params.file, Partition.None, 0L)
      metadata.removeFile(file)
      Command.user.info(s"Unregistered file ${storage.context.root.resolve(file.file)}")
    }
  }

  private class MigrateCommand extends FsDataStoreCommand {

    import scala.collection.JavaConverters._

    override val params = new MigrateParams()

    override val name: String = "migrate"

    override def execute(): Unit = withDataStore { ds =>
      val newParams = {
        val builder = Map.newBuilder[String, String]
        builder += (FileSystemDataStoreParams.MetadataTypeParam.getName -> params.newMetadataType)
        val metadataProps = new Properties()
        if (params.newMetadataConfigFile != null) {
          WithClose(new FileReader(params.newMetadataConfigFile))(metadataProps.load)
        }
        if (!params.newMetadataConfig.isEmpty) {
          params.newMetadataConfig.asScala.foreach { case (k, v) => metadataProps.put(k, v) }
        }
        if (!metadataProps.isEmpty) {
          val out = new StringWriter()
          metadataProps.store(out, null)
          builder += (FileSystemDataStoreParams.ConfigParam.getName -> out.toString)
        }
        builder.result()
      }
      val metadata = ds.storage(params.featureName).metadata
      val files = metadata.getFiles()
      val scheme = metadata.schemes.map(_.name).mkString(",")
      WithClose(DataStoreFinder.getDataStore((connection ++ newParams).asJava).asInstanceOf[FileSystemDataStore]) { newDs =>
        val sft = SimpleFeatureTypes.copy(ds.getSchema(params.featureName))
        sft.getUserData.put(StorageKeys.SchemeKey, scheme)
        newDs.createSchema(sft)
        newDs.storage(params.featureName).metadata.replaceFiles(Seq.empty, files.reverse)
      }
      Command.output.info(s"Migration complete for ${files.length} files")
    }
  }

  private class CheckConsistencyCommand extends FsDataStoreCommand with LazyLogging {

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
      private val onDisk = Collections.newSetFromMap(new ConcurrentHashMap[URI, java.lang.Boolean]())

      override def run(): Unit = {
        // list out the files currently in the root directory, results go into onDisk
        listRoot()

        val inconsistencies = ArrayBuffer.empty[Inconsistency]
        val checked = scala.collection.mutable.Set.empty[String]

        // compare the files known to the metadata to the ones on disk
        storage.metadata.getFiles().foreach { file =>
          if (onDisk.remove(storage.context.root.resolve(file.file))) {
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
                onDisk.remove(s.context.root.resolve(f.file))
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
        val iter = storage.fs.list(storage.context.root)
        // use a phaser to track worker thread completion
        val phaser = new Phaser(2) // 1 for this thread + 1 for the worker
        pool.submit(new TopLevelListWorker(phaser, iter))
        // wait for the worker threads to complete
        phaser.awaitAdvanceInterruptibly(phaser.arrive())
      }

      private class TopLevelListWorker(phaser: Phaser, list: CloseableIterator[URI]) extends Runnable {
        override def run(): Unit = {
          try {
            var i = phaser.getRegisteredParties + 1
            while (list.hasNext && i < PhaserUtils.MaxParties) {
              val path = list.next
              if (path.toString.endsWith("/")) { // .isDirectory
                if (!path.toString.endsWith(s"/${FileBasedMetadataCatalog.MetadataDirectory}/")) {
                  i += 1
                  // use a tiered phaser on each directory avoid the limit of 65535 registered parties
                  pool.submit(new ListWorker(new Phaser(phaser, 1), storage.fs.list(path)))
                }
              } else if (!storage.fs.filename(path).startsWith(".")) { // ignore "hidden" files
                onDisk.add(path)
              }
            }
            if (list.hasNext) {
              pool.submit(new TopLevelListWorker(new Phaser(phaser, 1), list))
            } else {
              list.close()
            }
          } catch {
            case NonFatal(e) => Try(list.close()); throw e
          } finally {
            phaser.arriveAndDeregister()
          }
        }
      }

      private class ListWorker(phaser: Phaser, listDirectory: => Iterator[URI]) extends Runnable {
        override def run(): Unit = {
          try {
            var i = phaser.getRegisteredParties + 1
            val iter = listDirectory
            while (iter.hasNext && i < PhaserUtils.MaxParties) {
              val path = iter.next
              if (path.toString.endsWith("/")) { // .isDirectory
                i += 1
                // use a tiered phaser on each directory avoid the limit of 65535 registered parties
                pool.submit(new ListWorker(new Phaser(phaser, 1), storage.fs.list(path)))
              } else if (!storage.fs.filename(path).startsWith(".")) { // ignore "hidden" files
                onDisk.add(path)
              }
            }
            if (iter.hasNext) {
              pool.submit(new ListWorker(new Phaser(phaser, 1), iter))
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
    @Parameter(description = "Path of the file(s) to register", required = true)
    var files: java.util.List[String] = new java.util.ArrayList[String]()

    @Parameter(names = Array("--delete"), description = "Delete file(s) after registering them")
    var delete: java.lang.Boolean = false
  }

  @Parameters(commandDescription = "Unregister data files from a storage instance")
  // noinspection VarCouldBeVal
  private class UnregisterParams extends FsParams with RequiredTypeNameParam {
    @Parameter(description = "Path of the file to unregister, relative to the storage root", required = true)
    var file: String = _
  }

  @Parameters(commandDescription = "Migrate metadata from one type to another")
  // noinspection VarCouldBeVal
  private class MigrateParams extends FsParams with RequiredTypeNameParam {

    @Parameter(
      names = Array("--new-metadata-type"),
      description = "Metadata type to migrate to",
      required = true,
      validateValueWith = Array(classOf[MetadataTypeValidator]))
    var newMetadataType: String = _

    @Parameter(
      names = Array("--new-metadata-config"),
      description = "Metadata configuration properties for the type to migrate to, in the form k=v",
      converter = classOf[KeyValueConverter],
      splitter = classOf[NoopParameterSplitter])
    var newMetadataConfig: java.util.List[(String, String)] = new java.util.ArrayList[(String, String)]()

    @Parameter(
      names = Array("--new-metadata-config-file"),
      description = "Name of a metadata configuration file for the type to migrate to, in Java properties format")
    var newMetadataConfigFile: File = _
  }

  @Parameters(commandDescription = "Check consistency between metadata and data files")
  // noinspection VarCouldBeVal
  private class CheckConsistencyParams extends FsParams with RequiredTypeNameParam {

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
