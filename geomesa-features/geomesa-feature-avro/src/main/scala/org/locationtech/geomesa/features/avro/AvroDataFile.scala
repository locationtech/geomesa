/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features.avro

import org.apache.avro.file.{DataFileStream, DataFileWriter}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object AvroDataFile {
  val SftNameKey = "sft.name"
  val SftSpecKey = "sft.spec"
  val VersionKey = "version"
  val Version: Long = 1L

  def setMetaData(dfw: DataFileWriter[SimpleFeature], sft: SimpleFeatureType): Unit = {
    dfw.setMeta(VersionKey, Version)
    dfw.setMeta(SftNameKey, sft.getTypeName)
    dfw.setMeta(SftSpecKey, SimpleFeatureTypes.encodeType(sft))
  }

  def canParse(dfs: DataFileStream[_ <: SimpleFeature]): Boolean = {
    dfs.getMetaKeys.contains(VersionKey) && dfs.getMetaLong(VersionKey) == Version
  }

  def getSft(dfs: DataFileStream[_ <: SimpleFeature]): SimpleFeatureType = {
    val sftName = dfs.getMetaString(SftNameKey)
    val sftString = dfs.getMetaString(SftSpecKey)
    SimpleFeatureTypes.createType(sftName, sftString)
  }
}

