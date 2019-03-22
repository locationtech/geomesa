/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools

import java.util
import java.util.ServiceLoader

import com.beust.jcommander.{IValueValidator, Parameter, ParameterException}
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.data.FileSystemDataStoreFactory.FileSystemDataStoreParams
import org.locationtech.geomesa.fs.storage.api.FileSystemStorageFactory
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

  import scala.collection.JavaConverters._

  trait FsParams {
    @Parameter(names = Array("--path", "-p"), description = "Path to root of filesystem datastore", required = true)
    var path: String = _

    @Parameter(names = Array("--config"), description = "Configuration properties, in the form k=v", variableArity = true)
    var configuration: java.util.List[String] = _
  }

  trait PartitionParam {
    @Parameter(names = Array("--partitions"), description = "Partitions to operate on (if empty all partitions will be used)", variableArity = true)
    var partitions: java.util.List[String] = new util.ArrayList[String]()
  }

  trait OptionalEncodingParam {
    @Parameter(names = Array("--encoding", "-e"), description = "Encoding (parquet, orc, converter, etc)", validateValueWith = classOf[EncodingValidator])
    var encoding: String = _
  }

  trait OptionalSchemeParams {
    @Parameter(names = Array("--partition-scheme"), description = "PartitionScheme typesafe config string or file")
    var scheme: java.lang.String = _

    @Parameter(names = Array("--leaf-storage"), description = "Use Leaf Storage for Partition Scheme", arity = 1)
    var leafStorage: java.lang.Boolean = true

    @Parameter(names = Array("--storage-opt"), variableArity = true, description = "Additional storage opts (k=v)", converter = classOf[KeyValueConverter])
    var storageOpts: java.util.List[(String, String)] = new java.util.ArrayList[(String, String)]()
  }

  class EncodingValidator extends IValueValidator[String] {
    override def validate(name: String, value: String): Unit = {
      val encodings = ServiceLoader.load(classOf[FileSystemStorageFactory]).asScala.map(_.encoding).toList
      if (!encodings.exists(_.equalsIgnoreCase(value))) {
        throw new ParameterException(s"$value is not a valid encoding for parameter $name." +
            s"Available encodings are: ${encodings.mkString(", ")}")
      }
    }
  }
}
