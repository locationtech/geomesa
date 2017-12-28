/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.hbase.tools

import java.io.File
import java.net.{MalformedURLException, URL}

import com.beust.jcommander.ParameterException
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.fs.hbase.tools.FsHBaseDataStoreCommand.FsHBaseParams
import org.locationtech.geomesa.fs.tools.{EncodingParam, FsDataStoreCommand, PathParam}
import org.locationtech.geomesa.fs.{FileSystemDataStore, FileSystemDataStoreParams}
import org.locationtech.geomesa.hbase.data.{HBaseDataStore, HBaseDataStoreParams}
import org.locationtech.geomesa.tools.{CatalogParam, Command}

/**
 * Abstract class for commands that have a pre-existing catalog
 */
trait FsHBaseDataStoreCommand extends Command {

  override def params: FsHBaseParams

  def fsConnection: Map[String, String] = {
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

  def hbaseConnection: Map[String, String] = Map(HBaseDataStoreParams.HBaseCatalogParam.getName -> params.catalog)

  @throws[ParameterException]
  def withDataStore[T](method: (HBaseDataStore, FileSystemDataStore) => T): T = {
    import scala.collection.JavaConversions._
    val hbaseDs = Option(DataStoreFinder.getDataStore(hbaseConnection).asInstanceOf[HBaseDataStore])
        .getOrElse(throw new ParameterException("Unable to create data store, please check your connection parameters."))

    try {
      val fsDs = Option(DataStoreFinder.getDataStore(fsConnection).asInstanceOf[FileSystemDataStore])
          .getOrElse(throw new ParameterException("Unable to create data store, please check your connection parameters."))
      try {
        method(hbaseDs, fsDs)
      } finally {
        fsDs.dispose()
      }
    } finally {
      hbaseDs.dispose()
    }
  }
}

object FsHBaseDataStoreCommand {

  private var urlStreamHandlerSet = false

  def configureURLFactory(): Unit =
    synchronized {
      if (!urlStreamHandlerSet) {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
        urlStreamHandlerSet = true
      }
    }

  trait FsHBaseParams extends PathParam with EncodingParam with CatalogParam
}
