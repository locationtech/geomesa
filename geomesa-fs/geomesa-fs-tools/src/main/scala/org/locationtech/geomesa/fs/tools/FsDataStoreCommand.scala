/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools

import com.beust.jcommander.{IValueValidator, Parameter, ParameterException}
import org.apache.hadoop.conf.Configuration
import org.locationtech.geomesa.fs.data.{FileSystemDataStore, FileSystemDataStoreParams}
import org.locationtech.geomesa.fs.storage.api.FileSystemStorageFactory
import org.locationtech.geomesa.fs.storage.common.metadata.{ConverterMetadata, FileBasedMetadata, JdbcMetadata}
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.tools.utils.NoopParameterSplitter
import org.locationtech.geomesa.tools.utils.ParameterConverters.{BytesValidator, KeyValueConverter}
import org.locationtech.geomesa.tools.{DataStoreCommand, DistributedCommand}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}

import java.io.{File, FileReader, StringWriter}
import java.util
import java.util.Properties

/**
 * Abstract class for FSDS commands
 */
trait FsDataStoreCommand extends DataStoreCommand[FileSystemDataStore] {

  import scala.collection.JavaConverters._

  override def params: FsParams

  override def connection: Map[String, String] = {
    val url = PathUtils.getUrl(params.path)
    val builder = Map.newBuilder[String, String]
    builder += (FileSystemDataStoreParams.PathParam.getName -> url.toString)
    builder += (FileSystemDataStoreParams.MetadataTypeParam.getName -> params.metadataType)
    val metadataProps = new Properties()
    if (params.metadataConfigFile != null) {
      WithClose(new FileReader(params.metadataConfigFile))(metadataProps.load)
    }
    if (!params.metadataConfig.isEmpty) {
      params.metadataConfig.asScala.foreach { case (k, v) => metadataProps.put(k, v) }
    }
    if (!metadataProps.isEmpty) {
      val out = new StringWriter()
      metadataProps.store(out, null)
      builder += (FileSystemDataStoreParams.MetadataConfigParam.getName -> out.toString)
    }

    if (!params.configuration.isEmpty) {
      val xml = {
        val conf = new Configuration(false)
        params.configuration.asScala.foreach { case (k, v) => conf.set(k, v) }
        val out = new StringWriter()
        conf.writeXml(out)
        out.toString
      }
      builder += (FileSystemDataStoreParams.ConfigsParam.getName -> xml)
    }
    if (params.auths != null) {
      builder += (FileSystemDataStoreParams.AuthsParam.getName -> params.auths)
    }
    if (params.encoding != null) {
      builder += (FileSystemDataStoreParams.EncodingParam.getName -> params.encoding)
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

    @Parameter(names = Array("--encoding", "-e"), description = "File encoding to use", validateValueWith = Array(classOf[EncodingValidator]))
    var encoding: String = _

    @Parameter(names = Array("--metadata-type"), description = "Metadata type to use", required = true, validateValueWith = Array(classOf[MetadataTypeValidator]))
    var metadataType: String = _

    @Parameter(
      names = Array("--metadata-config"),
      description = "Metadata configuration properties, in the form k=v",
      converter = classOf[KeyValueConverter],
      splitter = classOf[NoopParameterSplitter])
    var metadataConfig: java.util.List[(String, String)] = new java.util.ArrayList[(String, String)]()

    @Parameter(
      names = Array("--metadata-config-file"),
      description = "Name of a metadata configuration file, in Java properties format")
    var metadataConfigFile: File = _

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
    @Parameter(names = Array("--partition"), description = "Partition to operate on (if empty all partitions will be used)")
    var partitions: java.util.List[String] = new util.ArrayList[String]()
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

  class EncodingValidator extends IValueValidator[String] {
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
      val valid = Seq(FileBasedMetadata.MetadataType, JdbcMetadata.MetadataType, ConverterMetadata.MetadataType)
      if (!valid.contains(value)) {
        throw new ParameterException(s"$value is not a valid type for parameter $name." +
          s"Available metadata types are: ${valid.mkString(", ")}")
      }
    }
  }
}
