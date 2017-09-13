/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools

import java.io.File
import java.net.{MalformedURLException, URL}
import java.util
import java.util.concurrent.atomic.AtomicBoolean

import com.beust.jcommander.{Parameter, ParameterException}
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.locationtech.geomesa.fs.{FileSystemDataStore, FileSystemDataStoreParams}
import org.locationtech.geomesa.tools.DataStoreCommand

/**
 * Abstract class for commands that have a pre-existing catalog
 */
trait FsDataStoreCommand extends DataStoreCommand[FileSystemDataStore] {

  override def params: FsParams

  override def connection: Map[String, String] = {
    FsDataStoreCommand.configureURLFactory()
    val url = if (params.path.matches("""\w+://.*""")) {
      try {
        new URL(params.path)
      } catch {
        case e: MalformedURLException => throw new ParameterException(s"Invalid URL ${params.path}: ", e)
      }
    } else {
      try {
        new File(params.path).toURI.toURL
      } catch {
        case e: MalformedURLException => throw new ParameterException(s"Invalid URL ${params.path}: ", e)
      }
    }
    Map(FileSystemDataStoreParams.PathParam.getName -> url.toString,
      FileSystemDataStoreParams.EncodingParam.getName -> params.encoding)
  }
}

object FsDataStoreCommand {
  val facSet = new AtomicBoolean(false)
  def configureURLFactory(): Unit =
    synchronized {
      if (!facSet.get()) {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
        facSet.set(true)
      }
    }
}

trait PathParam {
  @Parameter(names = Array("--path", "-p"), description = "Path to root of filesystem datastore", required = true)
  var path: String = _
}

// TODO future work would be nice to store this in metadata
trait EncodingParam {
  @Parameter(names = Array("--encoding", "-e"), description = "Encoding (parquet, csv, etc)", required = true)
  var encoding: String = _
}


trait PartitionParam {
  @Parameter(names = Array("--partitions"), description = "Partitions (if empty all partitions will be used)", required = false, variableArity = true)
  var partitions: java.util.List[String] = new util.ArrayList[String]()
}

trait FsParams extends PathParam with EncodingParam
