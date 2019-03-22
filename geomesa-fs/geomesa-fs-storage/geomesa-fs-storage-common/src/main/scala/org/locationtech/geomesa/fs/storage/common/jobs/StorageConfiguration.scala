/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.jobs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
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
  val PartitionsKey = "geomesa.fs.partitions"
  val FileTypeKey   = "geomesa.fs.output.file-type"
  val SftNameKey    = "geomesa.fs.sft.name"
  val SftSpecKey    = "geomesa.fs.sft.spec"

  def setSft(conf: Configuration, sft: SimpleFeatureType): Unit = {
    val name = Option(sft.getName.getNamespaceURI).map(ns => s"$ns:${sft.getTypeName}").getOrElse(sft.getTypeName)
    conf.set(SftNameKey, name)
    conf.set(SftSpecKey, SimpleFeatureTypes.encodeType(sft, includeUserData = true))
  }
  def getSft(conf: Configuration): SimpleFeatureType =
    SimpleFeatureTypes.createType(conf.get(SftNameKey), conf.get(SftSpecKey))

  def setRootPath(conf: Configuration, path: Path): Unit = conf.set(PathKey, path.toString)
  def getRootPath(conf: Configuration): Path = new Path(conf.get(PathKey))

  def setPartitions(conf: Configuration, partitions: Array[String]): Unit =
    conf.setStrings(PartitionsKey, partitions: _*)
  def getPartitions(conf: Configuration): Array[String] = conf.getStrings(PartitionsKey)

  def setFileType(conf: Configuration, fileType: FileType): Unit = conf.set(FileTypeKey, fileType.toString)
  def getFileType(conf: Configuration): FileType = FileType.withName(conf.get(FileTypeKey))
}

trait StorageConfiguration {
  def configureOutput(sft: SimpleFeatureType, job: Job): Unit
}
