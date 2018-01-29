/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.util.{Properties, ServiceLoader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

abstract class FileSystemStorageFactory[T <: FileSystemStorage]
    extends org.locationtech.geomesa.fs.storage.api.FileSystemStorageFactory {

  import FileSystemStorageFactory.{ConfParam, EncodingParam, PathParam}

  protected def build(path: Path,
                      conf: Configuration,
                      params: java.util.Map[String, java.io.Serializable]): T

  override def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean =
    PathParam.exists(params) && EncodingParam.exists(params) && encoding.equalsIgnoreCase(EncodingParam.lookup(params))

  override def build(params: java.util.Map[String, java.io.Serializable]): T = {
    import scala.collection.JavaConversions._
    val root = new Path(PathParam.lookup(params))
    val conf = new Configuration()
    ConfParam.lookupOpt(params).foreach { props =>
      props.foreach { case (k, v) => conf.set(k, v) }
    }
    build(root, conf, params)
  }
}

object FileSystemStorageFactory {

  val PathParam     = new GeoMesaParam[String]("fs.path", "Root of the filesystem hierarchy", optional = false)
  val EncodingParam = new GeoMesaParam[String]("fs.encoding", "Encoding of data", optional = false)
  val ConfParam     = new GeoMesaParam[Properties]("fs.config", "Values to set in the root Configuration, in Java properties format", largeText = true)

  def getFileSystemStorage(params: java.util.Map[String, java.io.Serializable]): FileSystemStorage =
    load().find(_.canProcess(params)).map(_.build(params)).orNull

  def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean = load().exists(_.canProcess(params))

  private def load(): Iterator[org.locationtech.geomesa.fs.storage.api.FileSystemStorageFactory] = {
    import scala.collection.JavaConversions._
    ServiceLoader.load(classOf[org.locationtech.geomesa.fs.storage.api.FileSystemStorageFactory]).iterator()
  }
}
