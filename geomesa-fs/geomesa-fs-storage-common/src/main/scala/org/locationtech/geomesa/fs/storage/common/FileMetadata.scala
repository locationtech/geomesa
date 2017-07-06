/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.io.IOException
import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.locationtech.geomesa.fs.storage.api.Metadata

import scala.collection.JavaConversions._

class FileMetadata(fs: FileSystem,
                   path: Path,
                   conf: Configuration) extends Metadata with LazyLogging {

  private var cached: List[String] = _

  private def load(): List[String] = {
    if (fs.exists(path)) {
      val in = path.getFileSystem(conf).open(path)
      try {
        import scala.collection.JavaConversions._
        IOUtils.readLines(in).toList
      } finally {
        in.close()
      }
    } else {
      throw new IOException(s"Unable to locate metadata file ${path.toString}")
    }
  }

  override def addPartition(partition: String): Unit =  addPartitions(List(partition))

  override def addPartitions(toAdd: java.util.List[String]): Unit = {
    val parts = (load() ++ toAdd).distinct
    val out = path.getFileSystem(conf).create(path, true)
    parts.foreach { p => out.writeBytes(p); out.write('\n') }
    out.hflush()
    out.hsync()
    out.close()
    cached = null
    logger.info(s"wrote ${parts.size} partitions to metadata file")
  }

  override def getPartitions: java.util.List[String] = {
    if (cached == null) cached = load()
    cached
  }
}
