/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.blob.core.handlers

import java.io.File
import java.util
import java.util.{UUID, Date}
import javax.imageio.spi.ServiceRegistry

import com.vividsolutions.jts.geom.Geometry
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.accumulo.util.{Z3FeatureIdGenerator, Z3UuidGenerator}
import org.locationtech.geomesa.blob.core.AccumuloBlobStore._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._
import scala.util.Try

object BlobStoreFileHandler {
  def buildSF(file: File, params: Map[String, String]): Option[SimpleFeature] = {
    val handlers = ServiceRegistry.lookupProviders(classOf[FileHandler])

    handlers.find(_.canProcess(file, params)).map(_.buildSF(file, params))
  }
}

object BlobStoreByteArrayHandler extends BlobStoreSimpleFeatureBuilder {
  def buildSF(params: util.Map[String, String]): SimpleFeature = {
    val date = getDate(params)
    val geom = getGeometry(params)
    val fileName = getFileName(params)
    buildBlobSF(fileName, geom, date)
  }

  def getDate(params: util.Map[String, String]): Date = {
    getDateFromParams(params).getOrElse(new Date())
  }

  def getDateFromParams(params: util.Map[String, String]): Option[Date] = {
    Try { new Date(params.get(dateFieldName).toLong) }.toOption
  }

  def getGeometry(params: util.Map[String, String]): Geometry = {
    WKTUtils.read(
      params.getOrElse(geomeFieldName, throw new Exception(s"Missing Geometry Information from $params"))
    )
  }

  def getFileName(params: util.Map[String, String]): String = {
    params.getOrElse(filenameFieldName, UUID.randomUUID().toString)
  }
}

trait AbstractFileHandler extends BlobStoreSimpleFeatureBuilder with FileHandler with BlobStoreFileName {

  override def buildSF(file: File, params: util.Map[String, String]): SimpleFeature = {
    val fileName = getFileName(file, params)
    val geom = getGeometry(file, params)
    val dtg = getDate(file, params)
    buildBlobSF(fileName, geom, dtg)
  }

  def getDate(file: File, params: util.Map[String, String]): Date = {
    getDateFromFile(file).orElse(getDateFromParams(params)).getOrElse(new Date())
  }

  def getDateFromFile(file: File): Option[Date] = None

  def getDateFromParams(params: util.Map[String, String]): Option[Date] = None

  def getGeometry(file: File, params: util.Map[String, String]): Geometry = {
    getGeometryFromFile(file).orElse(getGeometryFromParams(params)).getOrElse {
      throw new Exception(s"Could not get Geometry for $file with params $params.")
    }
  }

  def getGeometryFromFile(file: File): Option[Geometry] = None

  def getGeometryFromParams(params: util.Map[String, String]): Option[Geometry] = None
}

trait BlobStoreFileName {

  def getFileNameFromParams(params: util.Map[String, String]): Option[String] = {
    Option(params.get(filenameFieldName))
  }

  def getFileName(file: File, params: util.Map[String, String]): String = {
    getFileNameFromParams(params).getOrElse(file.getName)
  }

}

trait BlobStoreSimpleFeatureBuilder {
  val builderLocal: ThreadLocal[SimpleFeatureBuilder] = new ThreadLocal[SimpleFeatureBuilder] {
    override def initialValue(): SimpleFeatureBuilder = new SimpleFeatureBuilder(sft).featureUserData(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
  }

  val featureIdGenerator = new Z3FeatureIdGenerator

  def buildBlobSF(fileName: String, geom: Geometry, dtg: Date): SimpleFeature = {
    val z3id = Z3UuidGenerator.createUuid(geom, dtg.getTime)

    val builder = builderLocal.get()
    builder.set(filenameFieldName, fileName)
    builder.set(geomeFieldName, geom)
    builder.set(idFieldName, z3id)
    builder.set(dateFieldName, dtg)

    builder.buildFeature(z3id.toString)
  }
}