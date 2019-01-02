/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.jobs

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType.FileType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

object StorageConfiguration {

  object Counters {
    val Group    = "org.locationtech.geomesa.jobs.fs"
    val Features = "features"
    val Written  = "written"
    val Failed   = "failed"
  }

  val PathKey       = "geomesa.fs.path"
  val EncodingKey   = "geomesa.fs.encoding"
  val SftConfKey    = "geomesa.fs.sft.config"
  val PartitionsKey = "geomesa.fs.partitions"
  val ExtensionKey  = "geomesa.fs.output.file-extension"
  val FileTypeKey   = "geomesa.fs.output.file-type"

  def setSft(conf: Configuration, sft: SimpleFeatureType): Unit = {
    // TODO verify that partition scheme is included?
    // PartitionScheme.extractFromSft(sft)
    // This must be serialized as conf due to the spec's inability to serialize user data completely
    conf.set(SftConfKey,
      SimpleFeatureTypes.toConfigString(sft, includeUserData = true, concise = true, includePrefix = false, json = true))
  }

  def getSft(conf: Configuration): SimpleFeatureType =
    SimpleFeatureTypes.createType(ConfigFactory.parseString(conf.get(SftConfKey)))

  def setPath(conf: Configuration, path: String): Unit = conf.set(PathKey, path)
  def getPath(conf: Configuration): String = conf.get(PathKey)

  def setEncoding(conf: Configuration, encoding: String): Unit = conf.set(EncodingKey, encoding)
  def getEncoding(conf: Configuration): String = conf.get(EncodingKey)

  def setPartitions(conf: Configuration, partitions: Array[String]): Unit =
    conf.setStrings(PartitionsKey, partitions: _*)
  def getPartitions(conf: Configuration): Array[String] = conf.getStrings(PartitionsKey)

  def setFileType(conf: Configuration, fileType: FileType): Unit = conf.set(FileTypeKey, fileType.toString)
  def getFileType(conf: Configuration): FileType = FileType.withName(conf.get(FileTypeKey))

  def setFileExtension(conf: Configuration, extension: String): Unit = conf.set(ExtensionKey, extension)
  def getFileExtension(conf: Configuration): String = conf.get(ExtensionKey)
}

trait StorageConfiguration {
  def configureOutput(sft: SimpleFeatureType, job: Job): Unit
}
