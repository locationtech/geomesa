/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.blob.api.handlers

import java.util
import java.util.{Date, UUID}

import org.locationtech.jts.geom.Geometry
import org.locationtech.geomesa.blob.api.GeoMesaBlobStoreSFT._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._
import scala.util.Try

object ByteArrayHandler extends BlobStoreSimpleFeatureBuilder {
  def buildSimpleFeature(params: util.Map[String, String]): SimpleFeature = {
    val date = getDate(params)
    val geom = getGeometry(params)
    val fileName = getFileName(params)
    buildBlobSimpleFeature(fileName, geom, date)
  }

  def getDate(params: util.Map[String, String]): Date = {
    getDateFromParams(params).getOrElse(new Date())
  }

  def getDateFromParams(params: util.Map[String, String]): Option[Date] = {
    Try { new Date(params.get(DtgFieldName).toLong) }.toOption
  }

  def getGeometry(params: util.Map[String, String]): Geometry = {
    WKTUtils.read(
      params.getOrElse(GeomFieldName, throw new Exception(s"Missing Geometry Information from $params"))
    )
  }

  def getFileName(params: util.Map[String, String]): String = {
    params.getOrElse(FilenameFieldName, UUID.randomUUID().toString)
  }
}