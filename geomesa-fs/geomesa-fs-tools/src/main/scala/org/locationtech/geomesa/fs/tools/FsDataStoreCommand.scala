/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools

import com.beust.jcommander.converters.BaseConverter
import com.beust.jcommander.{IValueValidator, Parameter, ParameterException}
import org.locationtech.geomesa.fs.data.{FileSystemDataStore, FileSystemDataStoreParams}
import org.locationtech.geomesa.fs.storage.core.{FileSystemStorageFactory, Partition}
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.tools.utils.NoopParameterSplitter
import org.locationtech.geomesa.tools.utils.ParameterConverters.{BytesValidator, KeyValueConverter}
import org.locationtech.geomesa.tools.{DataStoreCommand, DistributedCommand}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

import java.io.{File, StringWriter}
import java.util.Properties
import scala.util.control.NonFatal

/**
 * Abstract class for FSDS commands
 */
trait FsDataStoreCommand extends DataStoreCommand[FileSystemDataStore] {

  import scala.collection.JavaConverters._

  override def params: FsParams

  override def connection: Map[String, String] = {
    val builder = Map.newBuilder[String, String]
    builder += (FileSystemDataStoreParams.PathParam.key -> params.path)
    builder += (FileSystemDataStoreParams.MetadataTypeParam.key -> params.metadataType)
    if (!params.configuration.isEmpty) {
      val props = new Properties()
      params.configuration.asScala.foreach { case (k, v) => props.put(k, v) }
      val out = new StringWriter()
      props.store(out, null)
      builder += (FileSystemDataStoreParams.ConfigParam.key -> out.toString)
    }
    if (params.configFile != null) {
      builder += (FileSystemDataStoreParams.ConfigFileParam.key -> params.configFile)
    }
    if (params.auths != null) {
      builder += (FileSystemDataStoreParams.AuthsParam.key -> params.auths)
    }
    if (params.encoding != null) {
      builder += (FileSystemDataStoreParams.EncodingParam.key -> params.encoding)
    }
    builder.result()
  }
}

object FsDataStoreCommand {

  trait FsDistributedCommand extends FsDataStoreCommand with DistributedCommand {

    abstract override def libjarsFiles: Seq[String] =
      Seq("org/locationtech/geomesa/fs/tools/fs-libjars.list") ++ super.libjarsFiles

    abstract override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
      () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_FS_HOME", "lib"),
      () => ClassPathUtils.getJarsFromClasspath()
    ) ++ super.libjarsPaths
  }

  trait FsParams {
    @Parameter(names = Array("--path", "-p"), description = "Path to root of filesystem datastore", required = true)
    var path: String = _

    @Parameter(
      names = Array("--encoding", "-e"),
      description = "File encoding to use",
      validateValueWith = Array(classOf[EncodingValidator]))
    var encoding: String = _

    @Parameter(
      names = Array("--metadata-type"),
      description = "Metadata type to use",
      required = true,
      validateValueWith = Array(classOf[MetadataTypeValidator]))
    var metadataType: String = _

    @Parameter(
      names = Array("--config-file"),
      description = "Name of a configuration file, in Java properties format")
    var configFile: String = _

    @Parameter(
      names = Array("--config"),
      description = "Configuration properties, in the form k=v",
      converter = classOf[KeyValueConverter],
      splitter = classOf[NoopParameterSplitter])
    var configuration: java.util.List[(String, String)] = new java.util.ArrayList[(String, String)]()

    @Parameter(names = Array("--auths"), description = "Authorizations used to read data")
    var auths: String = _
  }

  trait PartitionParam {
    @Parameter(
      names = Array("--partition"),
      description = "Partition(s) to operate on",
      converter = classOf[PartitionConverter],
      splitter = classOf[NoopParameterSplitter])
    var partitions: java.util.List[Partition] = new java.util.ArrayList[Partition]()
  }

  trait OptionalSchemeParams {
    @Parameter(names = Array("--partition-scheme"), description = "Partition scheme identifier")
    var scheme: String = _

    @Parameter(
      names = Array("--storage-opt"),
      description = "Additional storage options to set as SimpleFeatureType user data, in the form key=value",
      converter = classOf[KeyValueConverter],
      splitter = classOf[NoopParameterSplitter])
    var storageOpts: java.util.List[(String, String)] = new java.util.ArrayList[(String, String)]()

    @Parameter(
      names = Array("--target-file-size"),
      description = "Target size for data files",
      validateValueWith = Array(classOf[BytesValidator]))
    var targetFileSize: String = _
  }

  private class PartitionConverter(name: String) extends BaseConverter[Partition](name) {
    override def convert(value: String): Partition = {
      try { Partition(value) } catch {
        case NonFatal(e) => throw new ParameterException(getErrorString(value, s"format: $e"))
      }
    }
  }

  private class EncodingValidator extends IValueValidator[String] {
    override def validate(name: String, value: String): Unit = {
      val encodings = FileSystemStorageFactory.factories.map(_.encoding).toList
      if (!encodings.exists(_.equalsIgnoreCase(value))) {
        throw new ParameterException(s"$value is not a valid encoding for parameter $name." +
            s"Available encodings are: ${encodings.mkString(", ")}")
      }
    }
  }

  class MetadataTypeValidator extends IValueValidator[String] {
    override def validate(name: String, value: String): Unit = {
      val valid = FileSystemDataStoreParams.MetadataTypeParam.enumerations
      if (!valid.contains(value)) {
        throw new ParameterException(s"$value is not a valid type for parameter $name." +
          s"Available metadata types are: ${valid.mkString(", ")}")
      }
    }
  }
}
