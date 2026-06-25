/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import com.beust.jcommander.{Parameter, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.iceberg.DataFile
import org.locationtech.geomesa.fs.storage.core.Metadata
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.fs.tools.ingest.FsManageMetadataCommand.ConfigureCommand
import org.locationtech.geomesa.tools.utils.NoopParameterSplitter
import org.locationtech.geomesa.tools.utils.ParameterConverters.KeyValueConverter
import org.locationtech.geomesa.tools.{Command, CommandWithSubCommands, RequiredTypeNameParam}

import java.net.URI
import scala.util.control.NonFatal

class FsManageMetadataCommand extends CommandWithSubCommands {

  import FsManageMetadataCommand.{ManageMetadataParams, RegisterCommand, UnregisterCommand}

  override val name: String = "manage-metadata"
  override val params = new ManageMetadataParams
  override val subCommands: Seq[Command] = Seq(new RegisterCommand(), new UnregisterCommand(), new ConfigureCommand())
}

object FsManageMetadataCommand extends LazyLogging {

  import scala.collection.JavaConverters._

  private class RegisterCommand extends FsDataStoreCommand {

    override val name = "register"
    override val params = new RegisterParams()

    override def execute(): Unit = withDataStore { ds =>
      val storage = ds.storage(params.featureName)

      val paths = params.files.asScala.map { file =>
        val path = {
          val tmp = new URI(file)
          if (tmp.getScheme == null || tmp.getScheme.isEmpty) {
            new URI(storage.context.root.getScheme, tmp.getHost, tmp.getPath, tmp.getFragment)
          } else if (tmp.getScheme == storage.context.root.getScheme) {
            tmp
          } else {
            throw new IllegalArgumentException(
              s"File $file must be in the same filesystem as the storage context: ${storage.context.root}")
          }
        }
        if (!storage.table.io().newInputFile(path.toString).exists()) {
          throw new IllegalArgumentException(s"File $path does not exist")
        }
        path
      }

      def outputResult(file: DataFile): Unit = {
        Command.user.info(s"Registered file ${storage.context.root.resolve(file.location())} containing ${file.recordCount()} known features")
        // TODO: extract and display bounds from DataFile.lowerBounds()/upperBounds()
      }

      try {
        paths.foreach { path =>
          outputResult(storage.metadata.register(path))
          if (params.delete) {
            storage.table.io().deleteFile(path.toString)
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
      val uri = storage.context.root.resolve(params.file).toString
      storage.table.newDelete().deleteFile(uri).commit()
      Command.user.info(s"Unregistered file: $uri")
    }
  }

  private class ConfigureCommand extends FsDataStoreCommand {

    override val params = new ConfigureParams()

    override val name: String = "configure"

    override def execute(): Unit = withDataStore { ds =>
      val table = ds.storage(params.featureName).table
      params.conf.asScala.foreach { case (k, v) =>
        val key = k.trim
        val value = Option(v.trim).filterNot(_.isEmpty)
        val current = Metadata.get(table, key)
        Metadata.set(table, k, value.orNull)
        Command.output.info(s"Updated $key from '${current.getOrElse("<unset>")}' to '${value.getOrElse("<unset>")}'")
      }
    }
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


  @Parameters(commandDescription = "Configure storage-level options")
  // noinspection VarCouldBeVal
  private class ConfigureParams extends FsParams with RequiredTypeNameParam {
    @Parameter(
      names = Array("--set"),
      description = "Storage properties to set, in the form k=v",
      converter = classOf[KeyValueConverter],
      splitter = classOf[NoopParameterSplitter],
      required = true)
    var conf: java.util.List[(String, String)] = new java.util.ArrayList[(String, String)]()
  }
}
