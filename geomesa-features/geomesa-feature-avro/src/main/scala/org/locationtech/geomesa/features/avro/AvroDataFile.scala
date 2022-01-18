/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import org.apache.avro.file.{DataFileStream, DataFileWriter}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * AvroDataFiles are binary Avro files (see https://avro.apache.org/) that encode
  * SimpleFeatures using a custom avro schema per SimpleFeatureType. AvroDataFiles
  * are meant to:
  * 1. Provide binary longer-term storage in filesystems for SimpleFeatures
  * 2. Carry the SimpleFeatureType and feature name along with the data
  *    using avro metadata
  * 3. Be self-describing outside of Geotools as much as possible
  *
  * You may want to consider gzipping your avro data file for better compression
  *
  * Version 3 supports Bytes as a type in the SFT
  */
object AvroDataFile {

  val SftNameKey = "sft.name"
  val SftSpecKey = "sft.spec"
  val VersionKey = "version"

  private[avro] val Version: Long = 3L

  def setMetaData(dfw: DataFileWriter[SimpleFeature], sft: SimpleFeatureType): Unit = {
    dfw.setMeta(VersionKey, Version)
    dfw.setMeta(SftNameKey, sft.getTypeName)
    dfw.setMeta(SftSpecKey, SimpleFeatureTypes.encodeType(sft))
  }

  /**
    * Backwards compatible...Version 2 can parse v1
    *
    * @param dfs data file stream
    * @return
    */
  def canParse(dfs: DataFileStream[_]): Boolean = {
    dfs.getMetaKeys.contains(VersionKey) &&
      dfs.getMetaLong(VersionKey) <= Version &&
      dfs.getMetaString(SftNameKey) != null &&
      dfs.getMetaString(SftSpecKey) != null
  }

  def getSft(dfs: DataFileStream[_]): SimpleFeatureType = {
    val sftName = dfs.getMetaString(SftNameKey)
    val sftString = dfs.getMetaString(SftSpecKey)
    SimpleFeatureTypes.createType(sftName, sftString)
  }
}

