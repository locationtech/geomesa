package org.locationtech.geomesa.blob.core.handlers

import java.util
import java.util.{Date, UUID}

import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.blob.core.GeoMesaBlobStoreSFT._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._
import scala.util.Try


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