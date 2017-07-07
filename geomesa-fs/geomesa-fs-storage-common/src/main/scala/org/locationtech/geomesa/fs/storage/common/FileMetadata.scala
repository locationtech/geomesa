/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import java.util

import com.typesafe.config._
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.locationtech.geomesa.fs.storage.api.Metadata

import scala.collection.JavaConversions._


// TODO GEOMESA-1913 Use atomic file writing for metadata
class FileMetadata(fs: FileSystem,
                   path: Path,
                   conf: Configuration) extends Metadata with LazyLogging {

  private var cached: Map[String, List[String]] = _

  private def load(): Map[String, List[String]] = {
    if (fs.exists(path)) {
      val in = path.getFileSystem(conf).open(path)
      val str = try {
        IOUtils.toString(in)
      } finally {
        in.close()
      }
      val config = ConfigFactory.parseString(str, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
      val pconfig = config.getConfig("partitions")
      pconfig.root().entrySet().map { e =>
        val key = e.getKey
        key -> pconfig.getStringList(key).toList
      }.toMap
    } else Map.empty
  }

  override def addPartition(partition: String, files: java.util.List[String]): Unit =
    addPartitions(Map{ partition -> files})

  override def addPartitions(toAdd: java.util.Map[String, java.util.List[String]]): Unit = {
    import scala.collection.JavaConversions._
    import scala.collection.JavaConverters._
    val existing = load()
    val allKeys = existing.keySet ++ toAdd.keySet()

    // todo typesafe config wants java boooo
    val javaMap: java.util.Map[String, java.util.List[String]] = allKeys.toList.map { k =>
      val e: List[String] = existing.get(k).toList.flatten
      val n: List[String] = toAdd.getOrDefault(k, List.empty[String]).toList
      k -> e.++(n).distinct.asJava
    }.toMap.asJava

    val scalaMap: Map[String, List[String]] = allKeys.toList.map { k =>
      val e: List[String] = existing.get(k).toList.flatten
      val n: List[String] = toAdd.getOrDefault(k, List.empty[String]).toList
      k -> e.++(n).distinct
    }.toMap

    val config = ConfigFactory.empty().withValue("partitions", ConfigValueFactory.fromMap(javaMap))
    val out = path.getFileSystem(conf).create(path, true)
    out.writeBytes(config.root.render(ConfigRenderOptions.defaults().setComments(false).setFormatted(true).setJson(true).setOriginComments(false)))
    out.hflush()
    out.hsync()
    out.close()
    cached = scalaMap
    logger.info(s"wrote ${allKeys.size} partitions to metadata file")
  }

  override def getPartitions: java.util.List[String] = {
    if (cached == null) cached = load()
    cached.keys.toList
  }

  override def getFiles(partition: String): util.List[String] = {
    if (cached == null) cached = load()
    import scala.collection.JavaConverters._
    cached.getOrElse(partition, List.empty[String]).asJava
  }

  override def getNumStorageFiles: Int = cached.entrySet().flatMap(_.getValue).size

  override def getNumPartitions: Int = cached.keys.size
}
