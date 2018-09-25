/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools

import java.util

import com.beust.jcommander.{IValueValidator, Parameter, ParameterException}
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.FileSystemDataStoreFactory.FileSystemDataStoreParams
import org.locationtech.geomesa.fs.storage.common.FileSystemStorageFactory
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.tools.DataStoreCommand
import org.locationtech.geomesa.tools.utils.ParameterConverters.KeyValueConverter
import org.locationtech.geomesa.utils.io.PathUtils

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
    if (params.configuration != null && !params.configuration.isEmpty) {
      builder += (FileSystemDataStoreParams.ConfParam.getName -> params.configuration.asScala.mkString("\n"))
    }
    builder.result()
  }
}

object FsDataStoreCommand {

  trait FsParams {
    @Parameter(names = Array("--path", "-p"), description = "Path to root of filesystem datastore", required = true)
    var path: String = _

    @Parameter(names = Array("--config"), description = "Configuration properties, in the form k=v", required = false, variableArity = true)
    var configuration: java.util.List[String] = _
  }

  trait EncodingParam {
    @Parameter(names = Array("--encoding", "-e"), description = "Encoding (parquet, orc, converter, etc)", validateValueWith = classOf[EncodingValidator], required = true)
    var encoding: String = _
  }

  trait PartitionParam {
    @Parameter(names = Array("--partitions"), description = "Partitions to operate on (if empty all partitions will be used)", required = false, variableArity = true)
    var partitions: java.util.List[String] = new util.ArrayList[String]()
  }

  trait SchemeParams {
    @Parameter(names = Array("--partition-scheme"), description = "PartitionScheme typesafe config string or file", required = true)
    var scheme: java.lang.String = _

    @Parameter(names = Array("--leaf-storage"), description = "Use Leaf Storage for Partition Scheme", required = false, arity = 1)
    var leafStorage: java.lang.Boolean = true

    @Parameter(names = Array("--storage-opt"), variableArity = true, description = "Additional storage opts (k=v)", required = false, converter = classOf[KeyValueConverter])
    var storageOpts: java.util.List[(String, String)] = new java.util.ArrayList[(String, String)]()
  }

  class EncodingValidator extends IValueValidator[String] {
    override def validate(name: String, value: String): Unit = {
      try {
        FileSystemStorageFactory.factory(value)
      } catch {
        case _: IllegalArgumentException =>
          throw new ParameterException(s"$value is not a valid encoding for parameter $name." +
              s"Available encodings are: ${FileSystemStorageFactory.factories().map(_.getEncoding).mkString(", ")}")
      }
    }
  }
}
